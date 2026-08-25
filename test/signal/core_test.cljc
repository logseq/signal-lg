(ns signal.core-test
  (:require [clojure.test :refer [deftest is testing]]
            [signal.core :as sig]))

(type-record test-item
  (key :string)
  (value :int))

(defn item [key value]
  (record test-item (key key) (value value)))

(defmacro assert-equal [expected actual message]
  `(is (= ~expected ~actual) ~message))

(deftest test-constant-and-state-batching
  (let [scheduler (sig/scheduler)
        constant (sig/constant scheduler "ready")
        state (sig/state scheduler 0)
        observed (atom [])
        _subscription
        (sig/observe
         (sig/value state)
         (fn [value]
           (swap! observed conj value)
           true))]
    (assert-equal "ready" (sig/sample constant) "constant value")
    (assert-equal [0] @observed "observer receives mounted value")
    (sig/set! state 1)
    (sig/set! state 2)
    (sig/update! state (fn [value] (+ value 3)))
    (assert-equal 0 (sig/get state) "state remains stable before stabilization")
    (assert-equal 0 (sig/generation scheduler) "initial generation")
    (sig/stabilize! scheduler)
    (assert-equal 5 (sig/get state) "batched state result")
    (assert-equal [0 5] @observed "batched state emits once")
    (assert-equal 1 (sig/generation scheduler) "generation after work")
    (sig/stabilize! scheduler)
    (assert-equal 1 (sig/generation scheduler) "no-op stabilization")))

(deftest test-incremental-map
  (let [scheduler (sig/scheduler)
        left (sig/state scheduler 1)
        right (sig/state scheduler "x")
        unary-calls (atom 0)
        doubled
        (sig/map
         (fn [value]
           (swap! unary-calls inc)
           (* value 2))
         (sig/value left))
        binary-calls (atom 0)
        combined
        (sig/map
         (fn [number text]
           (swap! binary-calls inc)
           (str number ":" text))
         (sig/value left)
         (sig/value right))]
    (assert-equal 2 (sig/sample doubled) "initial unary map")
    (assert-equal "1:x" (sig/sample combined) "initial binary map")
    (assert-equal 1 @unary-calls "unary map initializes once")
    (assert-equal 1 @binary-calls "binary map initializes once")
    (sig/set! left 2)
    (sig/set! left 3)
    (sig/set! right "y")
    (sig/stabilize! scheduler)
    (assert-equal 6 (sig/sample doubled) "unary map stabilized value")
    (assert-equal "3:y" (sig/sample combined) "binary map stabilized value")
    (assert-equal 2 @unary-calls "unary map recomputes once per batch")
    (assert-equal 2 @binary-calls "binary map recomputes once per batch")))

(deftest test-stabilization-diagnostics-count-scheduled-work
  (let [scheduler (sig/scheduler)
        source (sig/state scheduler 1)
        derived (sig/map (fn [value] (* value 2)) (sig/value source))
        observed (atom [])
        _subscription
        (sig/observe
         derived
         (fn [value]
           (swap! observed conj value)
           true))]
    (sig/set! source 2)
    (sig/set! source 3)
    (sig/stabilize! scheduler)
    (let [diagnostics (sig/last-stabilization scheduler)]
      (assert-equal 1 (:stabilization-generation diagnostics)
                    "work advances one scheduler generation")
      (assert-equal 2 (:stabilization-rounds diagnostics)
                    "state publication and derived recomputation use two rounds")
      (assert-equal 0 (:stabilization-effects diagnostics)
                    "the update schedules no effects")
      (assert-equal 2 (:stabilization-dirty-tasks diagnostics)
                    "batched writes publish once and recompute once"))
    (assert-equal [2 6] @observed "the derived observer runs only once")
    (sig/stabilize! scheduler)
    (let [diagnostics (sig/last-stabilization scheduler)]
      (assert-equal 1 (:stabilization-generation diagnostics)
                    "a no-op keeps the scheduler generation")
      (assert-equal 0 (:stabilization-rounds diagnostics)
                    "a no-op reports zero rounds")
      (assert-equal 0 (:stabilization-effects diagnostics)
                    "a no-op reports zero effects")
      (assert-equal 0 (:stabilization-dirty-tasks diagnostics)
                    "a no-op reports zero dirty work"))))

(deftest test-map-rejects-mixed-schedulers
  (let [left (sig/constant (sig/scheduler) 1)
        right (sig/constant (sig/scheduler) 2)
        rejected
        (try
          (do
            (sig/map + left right)
            false)
          (catch (Invalid_argument _message) true))]
    (is rejected "map inputs must share one scheduler")))

(deftest test-cutoff-and-subscription-cleanup
  (let [scheduler (sig/scheduler)
        state (sig/state scheduler 10)
        by-decade
        (sig/cutoff
         (fn [left right] (= (quot left 10) (quot right 10)))
         (sig/value state))
        observed (atom [])
        subscription
        (sig/observe
         by-decade
         (fn [value]
           (swap! observed conj value)
           true))]
    (sig/set! state 11)
    (sig/stabilize! scheduler)
    (assert-equal [10] @observed "cutoff suppresses equivalent changes")
    (sig/set! state 20)
    (sig/stabilize! scheduler)
    (assert-equal [10 20] @observed "cutoff forwards meaningful changes")
    (sig/dispose-subscription! subscription)
    (sig/dispose-subscription! subscription)
    (sig/set! state 30)
    (sig/stabilize! scheduler)
    (assert-equal [10 20] @observed "disposed observer stays detached")))

(deftest test-derived-signal-disposal
  (let [scheduler (sig/scheduler)
        source (sig/state scheduler 1)
        calls (atom 0)
        derived
        (sig/map
         (fn [current]
           (swap! calls inc)
           (* current 2))
         (sig/value source))]
    (assert-equal 2 (sig/sample derived) "derived signal initializes")
    (assert-equal 1 @calls "transform runs for initialization")
    (sig/dispose-signal! derived)
    (sig/dispose-signal! derived)
    (sig/set! source 2)
    (sig/stabilize! scheduler)
    (assert-equal 2 (sig/sample derived) "disposed signal keeps its final value")
    (assert-equal 1 @calls "disposed signal detaches from its source")))

(deftest test-scope-owned-signal-disposal
  (let [scheduler (sig/scheduler)
        scope (sig/scope "derived")
        source (sig/state scheduler 1)
        calls (atom 0)
        derived
        (sig/own-signal!
         scope
         (sig/map
          (fn [current]
            (swap! calls inc)
            (* current 2))
          (sig/value source)))]
    (assert-equal 2 (sig/sample derived) "owned signal initializes")
    (sig/dispose-scope! scope)
    (sig/set! source 2)
    (sig/stabilize! scheduler)
    (assert-equal 1 @calls "scope disposal detaches owned signal")))

(deftest test-effect-queue
  (let [scheduler (sig/scheduler)
        state (sig/state scheduler 0)
        trace (atom [])]
    (sig/enqueue-effect!
     scheduler
     (fn []
       (swap! trace conj "first")
       (sig/set! state 1)
       (sig/enqueue-effect!
        scheduler
        (fn []
          (swap! trace conj "third")
          (sig/update! state (fn [value] (+ value 10)))
          true))
       true))
    (sig/enqueue-effect!
     scheduler
     (fn []
       (swap! trace conj "second")
       (sig/update! state inc)
       true))
    (sig/stabilize! scheduler)
    (assert-equal ["first" "second" "third"] @trace
                  "effects execute in FIFO order")
    (assert-equal 12 (sig/get state) "effects share one stabilization batch")
    (assert-equal 1 (sig/generation scheduler) "effect batch generation")))

(deftest test-scope-lifecycle-and-state-slots
  (let [scheduler (sig/scheduler)
        trace (atom [])
        parent (sig/scope "parent")
        child (sig/scope "child" parent)
        slot (sig/state-slot "count")]
    (sig/on-mount! parent
                   (fn [] (swap! trace conj "mount-parent") true))
    (sig/on-unmount! parent
                     (fn [] (swap! trace conj "unmount-parent") true))
    (sig/on-mount! child
                   (fn [] (swap! trace conj "mount-child") true))
    (sig/on-unmount! child
                     (fn [] (swap! trace conj "unmount-child") true))
    (sig/mount! parent)
    (sig/mount! child)
    (let [first-state (sig/state-at scheduler parent slot 7)]
      (sig/set! first-state 9)
      (sig/stabilize! scheduler)
      (let [second-state (sig/state-at scheduler parent slot 999)]
        (is (identical? first-state second-state)
                "state slot preserves identity")
        (assert-equal 9 (sig/get second-state)
                      "reused state slot ignores new initializer")))
    (sig/dispose-scope! parent)
    (sig/dispose-scope! parent)
    (assert-equal 0 (count @(:slot-states slot))
                  "scope disposal releases state slots")
    (is (not (sig/active? parent)) "disposed parent is inactive")
    (is (not (sig/active? child)) "disposing parent disposes child")
    (assert-equal
     ["mount-parent" "mount-child" "unmount-child" "unmount-parent"]
     @trace
     "scope lifecycle order")))

(deftest test-scope-disposal-boundaries
  (let [scheduler (sig/scheduler)
        parent (sig/scope "parent")
        slot (sig/state-slot "state")
        cleanup-calls (atom 0)]
    (sig/on-dispose!
     parent
     (fn []
       (swap! cleanup-calls inc)
       (sig/dispose-scope! parent)
       true))
    (sig/dispose-scope! parent)
    (assert-equal 1 @cleanup-calls "scope disposal is reentrant-safe")
    (let [child-rejected
          (try
            (do (sig/scope "late-child" parent) false)
            (catch (Invalid_argument _message) true))
          state-rejected
          (try
            (do (sig/state-at scheduler parent slot 0) false)
            (catch (Invalid_argument _message) true))]
      (is child-rejected "disposed scope rejects new children")
      (is state-rejected "disposed scope rejects new state"))))

(deftest test-switch-lifecycle
  (let [scheduler (sig/scheduler)
        parent (sig/scope "screen")
        selected (sig/state scheduler false)
        trace (atom [])]
    (sig/mount! parent)
    (let [switch
          (sig/switch
           parent
           (sig/value selected)
           =
           (fn [key]
             (let [name (if key "content" "loading")
                   branch (sig/scope name parent)]
               (sig/on-mount!
                branch
                (fn [] (swap! trace conj (str "mount-" name)) true))
               (sig/on-unmount!
                branch
                (fn [] (swap! trace conj (str "unmount-" name)) true))
               branch)))]
      (assert-equal ["mount-loading"] @trace "initial switch branch")
      (assert-equal 1 (count @(:cleanup-callbacks parent))
                    "parent owns only the active switch branch")
      (sig/set! selected false)
      (sig/stabilize! scheduler)
      (assert-equal ["mount-loading"] @trace "equal key does not remount")
      (sig/set! selected true)
      (sig/stabilize! scheduler)
      (assert-equal
       ["mount-loading" "unmount-loading" "mount-content"]
       @trace
       "switch replaces only its branch")
      (assert-equal 1 (count @(:cleanup-callbacks parent))
                    "replaced branch detaches from its parent")
      (sig/dispose-switch! switch)
      (assert-equal 0 (count @(:cleanup-callbacks parent))
                    "disposed switch leaves no retained branch")
      (assert-equal
       ["mount-loading" "unmount-loading" "mount-content" "unmount-content"]
       @trace
       "switch disposal unmounts active branch"))))

(deftest test-keyed-collection
  (let [scheduler (sig/scheduler)
        parent (sig/scope "list")
        items (sig/state scheduler [(item "a" 1) (item "b" 2) (item "c" 3)])
        patches (atom [])
        updates (atom [])
        unmounted (atom [])]
    (sig/mount! parent)
    (let [keyed
          (sig/keyed
           parent
           (sig/value items)
           :key
           compare
           (fn [current-signal]
             (let [key (:key (sig/sample current-signal))
                   child (sig/scope key parent)]
               (sig/own!
                child
                (sig/observe
                 current-signal
                 (fn [current]
                   (swap! updates conj
                          (str (:key current) ":" (:value current)))
                   true)))
               (sig/on-unmount!
                child
                (fn [] (swap! unmounted conj key) true))
               child))
           (fn [patch]
             (swap! patches conj patch)
             true))]
      (assert-equal 3 (count @patches) "initial keyed insert count")
      (assert-equal 3 (count @(:cleanup-callbacks parent))
                    "parent owns the visible keyed scopes")
      (assert-equal ["a:1" "b:2" "c:3"] @updates
                    "keyed items expose initial values")
      (match (nth @patches 0)
        (sig/Insert key index)
        (do
          (assert-equal "a" key "first inserted key")
          (assert-equal 0 index "first inserted index"))
        _ (is false "first patch is Insert"))
      (match (nth @patches 1)
        (sig/Insert key index)
        (do
          (assert-equal "b" key "second inserted key")
          (assert-equal 1 index "second inserted index"))
        _ (is false "second patch is Insert"))
      (match (nth @patches 2)
        (sig/Insert key index)
        (do
          (assert-equal "c" key "third inserted key")
          (assert-equal 2 index "third inserted index"))
        _ (is false "third patch is Insert"))
      (let [original-a (sig/keyed-find-scope keyed "a")]
        (reset! patches [])
        (sig/set! items [(item "c" 30) (item "a" 10) (item "b" 20)])
        (sig/stabilize! scheduler)
        (assert-equal 1 (count @patches) "keyed reorder patch count")
        (match (nth @patches 0)
          (sig/Move key from-index to-index)
          (do
            (assert-equal "c" key "moved key")
            (assert-equal 2 from-index "move source index")
            (assert-equal 0 to-index "move target index"))
          _ (is false "reorder patch is Move"))
        (is (identical? original-a (sig/keyed-find-scope keyed "a"))
                "keyed move preserves scope identity")
        (assert-equal
         ["a:1" "b:2" "c:3" "c:30" "a:10" "b:20"]
         @updates
         "keyed scopes receive changed item values"))
      (reset! patches [])
      (sig/set! items [(item "c" 30) (item "d" 40) (item "a" 10)])
      (sig/stabilize! scheduler)
      (assert-equal 2 (count @patches) "keyed update patch count")
      (assert-equal 3 (count @(:cleanup-callbacks parent))
                    "removed keyed scopes detach from their parent")
      (match (nth @patches 0)
        (sig/Remove key index)
        (do
          (assert-equal "b" key "removed key")
          (assert-equal 2 index "removed index"))
        _ (is false "first keyed update patch is Remove"))
      (match (nth @patches 1)
        (sig/Insert key index)
        (do
          (assert-equal "d" key "inserted key")
          (assert-equal 1 index "inserted index"))
        _ (is false "second keyed update patch is Insert"))
      (assert-equal ["b"] @unmounted "removed scope is disposed")
      (sig/dispose-keyed! keyed)
      (assert-equal 0 (count @(:cleanup-callbacks parent))
                    "keyed disposal leaves no retained child scopes")
      (assert-equal ["b" "c" "d" "a"] @unmounted
                    "keyed disposal follows visible order"))))

(deftest test-keyed-duplicates
  (let [scheduler (sig/scheduler)
        parent (sig/scope "list")
        items (sig/state scheduler [(item "a" 1)])]
    (sig/mount! parent)
    (let [keyed
          (sig/keyed
           parent (sig/value items) :key compare
           (fn [current]
             (sig/scope (:key (sig/sample current)) parent))
           (fn [_patch] true))]
      (sig/set! items [(item "a" 1) (item "a" 2)])
      (let [rejected
            (try
              (do (sig/stabilize! scheduler) false)
              (catch (Invalid_argument _message) true))]
        (is rejected "duplicate keyed items are rejected"))
      (sig/dispose-keyed! keyed))))

(deftest test-keyed-lookup-uses-comparator
  (let [scheduler (sig/scheduler)
        parent (sig/scope "lookup")
        items (sig/constant scheduler [(item "a" 1)])
        keyed
        (sig/keyed
         parent items :key
         (fn [left right] (compare (count left) (count right)))
         (fn [current]
           (sig/scope (:key (sig/sample current)) parent))
         (fn [_patch] true))]
    (is (identical? (sig/keyed-find-scope keyed "a")
                    (sig/keyed-find-scope keyed "z"))
        "keyed lookup uses the configured comparator")
    (sig/dispose-keyed! keyed)))
