(lang dune 3.17)
(using melange 0.1)

(name signal-lg)
(version 0.1.0)

(generate_opam_files true)

(package
 (name signal-lg)
 (maintainers "Tienson Qin <tiensonqin@gmail.com>")
 (authors "Tienson Qin")
 (license MIT)
 (synopsis "A small statically typed incremental runtime for LG")
 (description
  "Reactive values, scoped state, lifecycle, effects, switches, and keyed collections for LG applications.")
 (depends
  (ocaml (>= 5.4))
  (dune (>= 3.17))
  (melange (>= 6.0))
  lg
  lg-test))
