open Lwt.Infix

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
    ; mutable write_promise: unit Lwt.t
    ; fd: Lwt_unix.file_descr
    ; channel: Lwt_io.output_channel }

  type op = P.op

  val sync : t -> t Lwt.t

  val of_file : string -> t Lwt.t

  val change : t -> op -> t
end = struct
  include P

  type t =
    { t: P.t
    ; mutable write_promise: unit Lwt.t
    ; fd: Lwt_unix.file_descr
    ; channel: Lwt_io.output_channel }

  let write t v =
    let p_len, p_blit = P.encode_blit v in
    let buf = Bytes.create (p_len + 8) in
    p_blit buf ~offset:8 ;
    EndianBytes.LittleEndian.set_int64 buf 0 (Int64.of_int p_len) ;
    t.write_promise <-
      ( t.write_promise
      >>= fun () -> Lwt_io.write_from_exactly t.channel buf 0 (Bytes.length buf)
      )

  let sync t =
    let write_promise = t.write_promise in
    write_promise
    >>= fun () ->
    (* All pending writes now complete *)
    Lwt_io.flush t.channel
    >>= fun () ->
    Lwt_unix.fsync t.fd
    >>= fun () ->
    Logs.debug (fun m -> m "Finished syncing") ;
    Lwt.return t

  let read_value channel =
    let rd_buf = Bytes.create 8 in
    Lwt_io.read_into_exactly channel rd_buf 0 8
    >>= fun () ->
    let size =
      EndianBytes.LittleEndian.get_int64 rd_buf 0 |> Int64.to_int
    in
    let payload_buf = Bytes.create size in
    Lwt_io.read_into_exactly channel payload_buf 0 size
    >>= fun () -> payload_buf |> Lwt.return

  let of_file file =
    Logs.debug (fun m -> m "Trying to open file") ;
    Lwt_unix.openfile file Lwt_unix.[O_RDONLY; O_CREAT] 0o640
    >>= fun fd ->
    let input_channel = Lwt_io.of_fd ~mode:Lwt_io.input fd in
    let stream =
      Lwt_stream.from (fun () ->
          Lwt.catch
            (fun () ->
              read_value input_channel
              >>= fun v -> decode v ~offset:0 |> Lwt.return_some)
            (function _ -> Lwt.return_none))
    in
    Logs.debug (fun m -> m "Reading in") ;
    Lwt_stream.fold (fun v t -> P.apply t v) stream (P.init ())
    >>= fun t ->
    Lwt_io.close input_channel
    >>= fun () ->
    Logs.debug (fun m -> m "Creating fd for persistance") ;
    Lwt_unix.openfile file Lwt_unix.[O_WRONLY; O_APPEND] 0o640
    >>= fun fd ->
    let channel = Lwt_io.of_fd ~mode:Lwt_io.output fd in
    Lwt.return {t; write_promise= Lwt.return_unit; fd; channel}

  let change t op =
    write t op ;
    {t with t= P.apply t.t op}
end
