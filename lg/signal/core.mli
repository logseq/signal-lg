(* ns signal.core *)

type stabilization_diagnostics = { stabilization_generation : int ; stabilization_rounds : int ; stabilization_effects : int ; stabilization_dirty_tasks : int }

type scheduler = { effects : (unit -> bool) Rrbvec.t ref ; dirty : (unit -> bool) Rrbvec.t ref ; generation_value : int ref ; last_stabilization_value : stabilization_diagnostics ref }

type subscription = { disposed : bool ref ; cancel : unit -> bool }

type 'value subscriber = { subscriber_id : int ; callback : 'value -> bool }

type cleanup_entry = { cleanup_id : int ; cleanup_callback : unit -> bool }

type 'value signal = { owner : scheduler ; current : 'value ref ; next_subscriber_id : int ref ; subscribers : 'value subscriber Rrbvec.t ref ; upstream_subscriptions : subscription Rrbvec.t ref ; disposed_signal : bool ref }

type 'value state = { state_signal : 'value signal ; pending : 'value option ref ; scheduled : bool ref }

type scope = { scope_id : int ; scope_name : string ; next_cleanup_id : int ref ; cleanup_callbacks : cleanup_entry Rrbvec.t ref ; mount_callbacks : (unit -> bool) Rrbvec.t ref ; unmount_callbacks : (unit -> bool) Rrbvec.t ref ; owned_subscriptions : subscription Rrbvec.t ref ; mounted : bool ref ; disposed_scope : bool ref }

type 'value state_slot = { slot_name : string ; slot_states : (int, 'value state) Lg_runtime.Runtime_map.t ref }

type 'key switch = { switch_subscription : subscription ; switch_scope : scope ref ; switch_disposed : bool ref }

type 'key keyed_patch =
  | Insert of 'key * int
  | Remove of 'key * int
  | Move of 'key * int * int

type ('key, 'item) keyed_entry = { entry_key : 'key ; entry_state : 'item state ; entry_scope : scope }

type ('key, 'item) keyed = { keyed_subscription : subscription ; keyed_entries : ('key, 'item) keyed_entry Rrbvec.t ref ; keyed_compare : 'key -> 'key -> int ; keyed_disposed : bool ref }

val scheduler : unit -> scheduler

val empty_callbacks : unit -> (unit -> bool) Rrbvec.t

val empty_cleanups : unit -> cleanup_entry Rrbvec.t

val empty_subscriptions : unit -> subscription Rrbvec.t

val generation : scheduler -> int

val last_stabilization : scheduler -> stabilization_diagnostics

val enqueue_effect_bang : scheduler -> (unit -> bool) -> bool

val enqueue_dirty_bang : scheduler -> (unit -> bool) -> bool

val schedule_once_bang : scheduler -> bool ref -> (unit -> bool) -> bool

val stabilize_bang : scheduler -> bool

val constant : scheduler -> 'value -> 'value signal

val state : scheduler -> 'value -> 'value state

val value : 'value state -> 'value signal

val sample : 'value signal -> 'value

val get : 'value state -> 'value

val set_bang : 'value state -> 'value -> bool

val update_bang : 'value state -> ('value -> 'value) -> bool

val observe : 'value signal -> ('value -> bool) -> subscription

val subscribe_ : 'value signal -> ('value -> bool) -> bool -> subscription

val dispose_subscription_bang : subscription -> bool

val dispose_signal_bang : 'value signal -> bool

val map : (('left -> 'output) -> 'left signal -> 'output signal) * ((('left -> 'right -> 'output) -> 'left signal -> 'right signal -> 'output signal) * unit)

val cutoff : ('value -> 'value -> bool) -> 'value signal -> 'value signal

val scope : (string -> scope) * ((string -> scope -> scope) * unit)

val make_scope : string -> scope

val on_mount_bang : scope -> (unit -> bool) -> bool

val on_unmount_bang : scope -> (unit -> bool) -> bool

val on_dispose_bang : scope -> (unit -> bool) -> bool

val register_cleanup_bang : scope -> (unit -> bool) -> subscription

val own_bang : scope -> subscription -> subscription

val own_signal_bang : scope -> 'value signal -> 'value signal

val mount_bang : scope -> bool

val dispose_scope_bang : scope -> bool

val active_ : scope -> bool

val state_slot : string -> 'value state_slot

val state_at : scheduler -> scope -> 'value state_slot -> 'value -> 'value state

val switch : scope -> 'key signal -> ('key -> 'key -> bool) -> ('key -> scope) -> 'key switch

val dispose_switch_bang : 'key switch -> bool

val keyed : scope -> 'item Rrbvec.t signal -> ('item -> 'key) -> ('key -> 'key -> int) -> ('item signal -> scope) -> ('key keyed_patch -> bool) -> ('key, 'item) keyed

val keyed_find_scope : ('key, 'item) keyed -> 'key -> scope

val dispose_keyed_bang : ('key, 'item) keyed -> bool

val find_entry_index : ('key, 'item) keyed_entry Rrbvec.t -> 'key -> ('key -> 'key -> int) -> int option

val key_index : 'item Rrbvec.t -> ('item -> 'key) -> ('key -> 'key -> int) -> ('key, int) persistent_tree_map

val remove_entry_at : ('key, 'item) keyed_entry Rrbvec.t -> int -> ('key, 'item) keyed_entry Rrbvec.t

val insert_entry_at : ('key, 'item) keyed_entry Rrbvec.t -> int -> ('key, 'item) keyed_entry -> ('key, 'item) keyed_entry Rrbvec.t

val move_entry : ('key, 'item) keyed_entry Rrbvec.t -> int -> int -> ('key, 'item) keyed_entry Rrbvec.t

val reconcile_keyed_bang : scheduler -> ('key, 'item) keyed_entry Rrbvec.t ref -> 'item Rrbvec.t -> ('item -> 'key) -> ('key -> 'key -> int) -> ('item signal -> scope) -> ('key keyed_patch -> bool) -> bool

