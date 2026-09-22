(ns signal.core
  (:refer-clojure :exclude [get map]))

(defn scheduler []
  (record scheduler
          (effects (atom []))
          (dirty (atom []))
          (generation-value (atom 0))
          (last-stabilization-value
           (atom
            (record stabilization-diagnostics
              (stabilization-generation 0)
              (stabilization-rounds 0)
              (stabilization-effects 0)
              (stabilization-dirty-tasks 0))))))

(defn generation [owner]
  (deref (:generation-value owner)))

(defn last-stabilization [owner]
  (deref (:last-stabilization-value owner)))

(defn enqueue-effect! [owner effect]
  (reset! (:effects owner)
          (conj (deref (:effects owner)) effect))
  true)

(defn- enqueue-dirty! [owner task]
  (reset! (:dirty owner)
          (conj (deref (:dirty owner)) task))
  true)

(defn- schedule-once! [owner scheduled task]
  (if (deref scheduled)
    true
    (do
      (reset! scheduled true)
      (enqueue-dirty!
       owner
       (fn []
         (reset! scheduled false)
         (task nil)))
      true)))

(defn stabilize! [owner]
  (let [worked (atom false)
        rounds (atom 0)
        effect-count (atom 0)
        dirty-count (atom 0)]
    (loop []
      (let [effects (deref (:effects owner))
            dirty (deref (:dirty owner))]
        (if (and (empty? effects) (empty? dirty))
          true
          (do
            (reset! worked true)
            (swap! rounds inc)
            (swap! effect-count + (count effects))
            (reset! (:effects owner) (empty-callbacks))
            (doseq [effect effects]
              (effect nil))
            (let [pending-dirty (deref (:dirty owner))]
              (swap! dirty-count + (count pending-dirty))
              (reset! (:dirty owner) (empty-callbacks))
              (doseq [task pending-dirty]
                (task nil)))
            (recur)))))
    (when (deref worked)
      (swap! (:generation-value owner) inc))
    (reset!
     (:last-stabilization-value owner)
     (record stabilization-diagnostics
       (stabilization-generation (generation owner))
       (stabilization-rounds (deref rounds))
       (stabilization-effects (deref effect-count))
       (stabilization-dirty-tasks (deref dirty-count))))
    true))

(defn constant [owner initial]
  (record signal
          (owner owner)
          (current (atom initial))
          (next-subscriber-id (atom 0))
          (subscribers (atom []))
          (upstream-subscriptions (atom (empty-subscriptions)))
          (disposed-signal (atom false))))

(defn sample [reactive]
  (deref (:current reactive)))

(defn- subscribe* [reactive callback emit-initial?]
  (when (deref (:disposed-signal reactive))
    (raise (Invalid_argument "cannot observe a disposed signal")))
  (let [subscriber-id (swap! (:next-subscriber-id reactive) inc)
        subscriber-value
        (record subscriber
                (subscriber-id subscriber-id)
                (callback callback))
        disposed (atom false)
        cancel
        (fn []
          (if (deref disposed)
            true
            (do
              (reset! disposed true)
              (swap!
               (:subscribers reactive)
               (fn [subscribers]
                 (filterv
                  (fn [current]
                    (not (= subscriber-id (:subscriber-id current))))
                  subscribers)))
              true)))]
    (swap! (:subscribers reactive) conj subscriber-value)
    (when emit-initial?
      (callback (sample reactive)))
    (record subscription
            (disposed disposed)
            (cancel cancel))))

(defn observe [reactive callback]
  (subscribe* reactive callback true))

(defn dispose-subscription! [subscription]
  ((:cancel subscription)))

(defn dispose-signal! [reactive]
  (if (deref (:disposed-signal reactive))
    true
    (do
      (reset! (:disposed-signal reactive) true)
      (doseq [subscription (deref (:upstream-subscriptions reactive))]
        (dispose-subscription! subscription))
      (reset! (:upstream-subscriptions reactive) (empty-subscriptions))
      (reset! (:subscribers reactive) [])
      true)))

(defn- publish! [reactive next-value]
  (when-not (deref (:disposed-signal reactive))
    (reset! (:current reactive) next-value)
    (doseq [subscriber-value (deref (:subscribers reactive))]
      ((:callback subscriber-value) next-value)))
  true)

(defn state [owner initial]
  (record state
          (state-signal (constant owner initial))
          (pending (atom None))
          (scheduled (atom false))))

(defn value [state-value]
  (:state-signal state-value))

(defn get [state-value]
  (sample (value state-value)))

(defn set! [state-value next-value]
  (reset! (:pending state-value) (Some next-value))
  (schedule-once!
   (:owner (value state-value))
   (:scheduled state-value)
   (fn []
     (match (deref (:pending state-value))
       (Some pending-value)
       (do
         (reset! (:pending state-value) None)
         (publish! (value state-value) pending-value))
       None true))))

(defn update! [state-value update-fn]
  (let [current
        (match (deref (:pending state-value))
          (Some pending-value) pending-value
          None (get state-value))]
    (set! state-value (update-fn current))))

(defn map
  ([transform source]
   (let [derived (constant (:owner source) (transform (sample source)))
         scheduled (atom false)
         subscription
         (subscribe*
          source
          (fn [_current]
            (schedule-once!
             (:owner source)
             scheduled
             (fn []
               (if (deref (:disposed-signal derived))
                 true
                 (publish! derived (transform (sample source)))))))
          false)]
     (swap! (:upstream-subscriptions derived) conj subscription)
     derived))
  ([transform left right]
   (when-not (identical? (:owner left) (:owner right))
     (raise (Invalid_argument "map inputs must share one scheduler")))
   (let [derived
         (constant
          (:owner left)
          (transform (sample left) (sample right)))
         scheduled (atom false)
         recompute
         (fn [_changed]
           (schedule-once!
            (:owner left)
            scheduled
            (fn []
              (publish!
               derived
               (transform (sample left) (sample right))))))]
     (swap! (:upstream-subscriptions derived)
            conj
            (subscribe* left recompute false))
     (swap! (:upstream-subscriptions derived)
            conj
            (subscribe* right recompute false))
     derived)))

(defn cutoff [equal source]
  (let [derived (constant (:owner source) (sample source))]
    (swap!
     (:upstream-subscriptions derived)
     conj
     (subscribe*
      source
      (fn [next-value]
        (if (equal (sample derived) next-value)
          true
          (publish! derived next-value)))
      false))
    derived))

(def next-scope-id (atom 0))

(defn- empty-callbacks [] [])
(defn- empty-cleanups [] [])
(defn- empty-subscriptions [] [])

(defn- make-scope [name]
  (record scope
          (scope-id (swap! next-scope-id inc))
          (scope-name name)
          (next-cleanup-id (atom 0))
          (cleanup-callbacks (atom (empty-cleanups)))
          (mount-callbacks (atom (empty-callbacks)))
          (unmount-callbacks (atom (empty-callbacks)))
          (owned-subscriptions (atom (empty-subscriptions)))
          (mounted (atom false))
          (disposed-scope (atom false))))

(defn- root-scope [name]
  (make-scope name))

(defn- child-scope [name parent]
  (when (deref (:disposed-scope parent))
    (raise (Invalid_argument "cannot create a child of a disposed scope")))
  (let [child (make-scope name)]
    (own! child
          (register-cleanup!
           parent
           (fn [] (dispose-scope! child))))
    child))

(defn scope
  ([name] (root-scope name))
  ([name parent] (child-scope name parent)))

(defn on-mount! [scope-value callback]
  (swap! (:mount-callbacks scope-value) conj callback)
  true)

(defn on-unmount! [scope-value callback]
  (swap! (:unmount-callbacks scope-value) conj callback)
  true)

(defn- register-cleanup! [scope-value callback]
  (if (deref (:disposed-scope scope-value))
    (do
      (callback nil)
      (record subscription
              (disposed (atom true))
              (cancel (fn [] true))))
    (let [cleanup-id (swap! (:next-cleanup-id scope-value) inc)
          disposed (atom false)
          entry
          (record cleanup-entry
                  (cleanup-id cleanup-id)
                  (cleanup-callback callback))
          cancel
          (fn []
            (if (deref disposed)
              true
              (do
                (reset! disposed true)
                (swap!
                 (:cleanup-callbacks scope-value)
                 (fn [entries]
                   (filterv
                    (fn [current]
                      (not (= cleanup-id (:cleanup-id current))))
                    entries)))
                true)))]
      (swap! (:cleanup-callbacks scope-value) conj entry)
      (record subscription
              (disposed disposed)
              (cancel cancel)))))

(defn on-dispose! [scope-value callback]
  (register-cleanup! scope-value callback)
  true)

(defn own! [scope-value subscription]
  (if (deref (:disposed-scope scope-value))
    (dispose-subscription! subscription)
    (do
      (swap! (:owned-subscriptions scope-value) conj subscription)
      true))
  subscription)

(defn own-signal! [scope-value reactive]
  (on-dispose!
   scope-value
   (fn [] (dispose-signal! reactive)))
  reactive)

(defn mount! [scope-value]
  (if (or (deref (:disposed-scope scope-value))
          (deref (:mounted scope-value)))
    true
    (do
      (reset! (:mounted scope-value) true)
      (doseq [callback (deref (:mount-callbacks scope-value))]
        (callback nil))
      true)))

(defn dispose-scope! [scope-value]
  (if (deref (:disposed-scope scope-value))
    true
    (do
      (reset! (:disposed-scope scope-value) true)
      (doseq [cleanup (deref (:cleanup-callbacks scope-value))]
        ((:cleanup-callback cleanup)))
      (doseq [subscription (deref (:owned-subscriptions scope-value))]
        (dispose-subscription! subscription))
      (when (deref (:mounted scope-value))
        (doseq [callback (deref (:unmount-callbacks scope-value))]
          (callback nil)))
      (reset! (:mounted scope-value) false)
      (reset! (:cleanup-callbacks scope-value) (empty-cleanups))
      (reset! (:owned-subscriptions scope-value) (empty-subscriptions))
      true)))

(defn active? [scope-value]
  (and (deref (:mounted scope-value))
       (not (deref (:disposed-scope scope-value)))))

(defn state-slot [name]
  (record state-slot
          (slot-name name)
          (slot-states (atom (hash-map)))))

(defn state-at [scheduler scope-value slot initial]
  (when (deref (:disposed-scope scope-value))
    (raise (Invalid_argument "cannot create state in a disposed scope")))
  (let [scope-id (:scope-id scope-value)]
    (if-some [existing (clojure.core/get (deref (:slot-states slot)) scope-id)]
      existing
      (let [created (state scheduler initial)]
        (swap! (:slot-states slot) assoc scope-id created)
        (on-dispose!
         scope-value
         (fn []
           (dispose-signal! (value created))
           (swap! (:slot-states slot) dissoc scope-id)
           true))
        created))))

(defn switch [parent source equal mount]
  (let [initial-key (sample source)
        current-key (atom initial-key)
        current-scope (atom (mount initial-key))
        disposed (atom false)]
    (mount! (deref current-scope))
    (let [subscription
          (subscribe*
           source
           (fn [next-key]
             (if (or (deref disposed)
                     (equal (deref current-key) next-key))
               true
               (do
                 (dispose-scope! (deref current-scope))
                 (let [next-scope (mount next-key)]
                   (reset! current-key next-key)
                   (reset! current-scope next-scope)
                   (mount! next-scope)
                   true))))
           false)
          switch-value
          (record switch
                  (switch-subscription subscription)
                  (switch-scope current-scope)
                  (switch-disposed disposed))]
      (own! parent subscription)
      switch-value)))

(defn dispose-switch! [switch-value]
  (if (deref (:switch-disposed switch-value))
    true
    (do
      (reset! (:switch-disposed switch-value) true)
      (dispose-subscription! (:switch-subscription switch-value))
      (dispose-scope! (deref (:switch-scope switch-value)))
      true)))

(defn- find-entry-index [entries key compare]
  (loop [index 0]
    (if (= index (count entries))
      None
      (if (= 0 (compare (:entry-key (nth entries index)) key))
        (Some index)
        (recur (inc index))))))

(defn- key-index [items key-fn compare]
  (loop [index 0
         indexes (sorted-map-by compare)]
    (if (= index (count items))
      indexes
      (let [key (key-fn (nth items index))]
        (if-some [_existing (clojure.core/tree-map-get indexes key)]
          (raise (Invalid_argument "keyed collection contains a duplicate key"))
          (recur
           (inc index)
           (clojure.core/tree-map-assoc indexes key index)))))))

(defn- remove-entry-at [entries removed-index]
  (loop [index 0
         result []]
    (if (= index (count entries))
      result
      (recur (inc index)
             (if (= index removed-index)
               result
               (conj result (nth entries index)))))))

(defn- insert-entry-at [entries inserted-index inserted]
  (loop [index 0
         result []]
    (if (= index (count entries))
      (if (= inserted-index index)
        (conj result inserted)
        result)
      (recur
       (inc index)
       (conj
        (if (= index inserted-index)
          (conj result inserted)
          result)
        (nth entries index))))))

(defn- move-entry [entries from-index to-index]
  (let [moving (nth entries from-index)
        without (remove-entry-at entries from-index)]
    (insert-entry-at without to-index moving)))

(defn- reconcile-keyed! [scheduler entries-ref items key-fn compare mount on-patch]
  (let [new-indexes (key-index items key-fn compare)
        without-removed
        (loop [index (dec (count (deref entries-ref)))
               entries (deref entries-ref)]
          (if (< index 0)
            entries
            (let [entry (nth entries index)
                  key (:entry-key entry)]
              (if-some [_new-index
                        (clojure.core/tree-map-get new-indexes key)]
                (recur (dec index) entries)
                (do
                  (on-patch (Remove key index))
                  (dispose-scope! (:entry-scope entry))
                  (recur (dec index) (remove-entry-at entries index)))))))]
    (let [reconciled
          (loop [index 0
                 entries without-removed]
            (if (= index (count items))
              entries
              (let [current (nth items index)
                    key (key-fn current)]
                (if (and (< index (count entries))
                         (= 0 (compare
                               (:entry-key (nth entries index)) key)))
                  (let [entry (nth entries index)]
                    (when-not (= (get (:entry-state entry)) current)
                      (set! (:entry-state entry) current))
                    (recur (inc index) entries))
                  (if-some [existing-index
                            (find-entry-index entries key compare)]
                    (let [entry (nth entries existing-index)]
                      (when-not (= (get (:entry-state entry)) current)
                        (set! (:entry-state entry) current))
                      (on-patch (Move key existing-index index))
                      (recur (inc index)
                             (move-entry entries existing-index index)))
                    (let [item-state (state scheduler current)
                          child (mount (value item-state))
                          entry
                          (record keyed-entry
                                  (entry-key key)
                                  (entry-state item-state)
                                  (entry-scope child))]
                      (mount! child)
                      (on-patch (Insert key index))
                      (recur (inc index)
                             (insert-entry-at entries index entry))))))))]
      (reset! entries-ref reconciled)
      true)))

(defn keyed [parent source key-fn compare mount on-patch]
  (let [scheduler (:owner source)
        initial-items (sample source)
        entries-ref (atom [])
        _initial
        (reconcile-keyed!
         scheduler entries-ref initial-items key-fn compare mount on-patch)
        disposed (atom false)
        subscription
        (subscribe*
         source
         (fn [items]
           (reconcile-keyed!
            scheduler entries-ref items key-fn compare mount on-patch))
         false)
        keyed-value
        (record keyed
                (keyed-subscription subscription)
                (keyed-entries entries-ref)
                (keyed-compare compare)
                (keyed-disposed disposed))]
    (own! parent subscription)
    keyed-value))

(defn keyed-find-scope [keyed-value key]
  (loop [index 0]
    (if (= index (count (deref (:keyed-entries keyed-value))))
      (raise (Invalid_argument "keyed scope not found"))
      (let [entry (nth (deref (:keyed-entries keyed-value)) index)]
        (if (= 0 ((:keyed-compare keyed-value) (:entry-key entry) key))
          (:entry-scope entry)
          (recur (inc index)))))))

(defn dispose-keyed! [keyed-value]
  (if (deref (:keyed-disposed keyed-value))
    true
    (do
      (reset! (:keyed-disposed keyed-value) true)
      (dispose-subscription! (:keyed-subscription keyed-value))
      (doseq [entry (deref (:keyed-entries keyed-value))]
        (dispose-scope! (:entry-scope entry)))
      true)))
