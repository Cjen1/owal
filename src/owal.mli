module type Persistable = sig
  type t

  val init : unit -> t

  type op

  val encode_blit : op -> int * (bytes -> offset:int -> unit)

  val decode : bytes -> offset:int -> op

  val apply : t -> op -> t
end

module Persistant (P : Persistable) : sig
  type t =
    { t: P.t
    ; write_promise: (unit, exn) Lwt_result.t
    ; fd: Lwt_unix.file_descr
    ; channel: Lwt_io.output_channel }

  type op = P.op

  val sync : t -> (unit, exn) Lwt_result.t

  val of_file : string -> t Lwt.t

  val change : t -> op -> t

  val close : t -> unit Lwt.t
end
