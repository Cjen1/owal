(lang dune 2.5)
(name owal)
(source (github cjen1/owal))
(authors "Chris Jensen")

(generate_opam_files true)

(package
  (name owal)
  (synopsis "Basic write-ahead logging in ocaml")
  (depends
    lwt
    logs
    fmt
    base
    ))

