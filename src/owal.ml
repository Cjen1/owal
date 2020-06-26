open Lwt.Infix

let src = Logs.Src.create "Owal"

module Log = (val Logs.src_log src : Logs.LOG)

let ( >>>= ) = Lwt_result.bind

module type Persistable = sig
  type t

  val init : unit -> t

  type op

  val encode_blit : op -> int * (bytes -> offset:int -> unit)

  val decode : bytes -> offset:int -> op

  val apply : t -> op -> t
end

module Persistant (P : Persistable) = struct
  include P

  type t =
    { t: P.t
    ; write_promise: (unit, exn) Lwt_result.t
    ; fd: Lwt_unix.file_descr
    ; channel: Lwt_io.output_channel }

  let write t v =
    let p_len, p_blit = P.encode_blit v in
    let buf = Bytes.create (p_len + 8) in
    p_blit buf ~offset:8 ;
    EndianBytes.LittleEndian.set_int64 buf 0 (Int64.of_int p_len) ;
    let write_p =
      t.write_promise
      >>>= fun () ->
      Lwt_io.write_from_exactly t.channel buf 0 (Bytes.length buf)
      >>= Lwt.return_ok
    in
    {t with write_promise= write_p}

  let sync t =
    let write_promise = t.write_promise in
    write_promise
    >>= function
    | Error exn ->
        Log.err (fun m -> m "Got error from write_promise %a" Fmt.exn exn) ;
        Lwt.return_error exn
    | Ok () ->
        (* All pending writes now complete *)
        Lwt_io.flush t.channel
        >>= fun () ->
        Lwt_unix.fsync t.fd
        >>= fun () ->
        Logs.debug (fun m -> m "Finished syncing") ;
        Lwt.return_ok ()

  let read_value channel =
    let rd_buf = Bytes.create 8 in
    Lwt_io.read_into_exactly channel rd_buf 0 8
    >>= fun () ->
    let size = EndianBytes.LittleEndian.get_int64 rd_buf 0 |> Int64.to_int in
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
    Lwt.return {t; write_promise= Lwt.return_ok (); fd; channel}

  let change op t = {(write t op) with t= P.apply t.t op}

  let close t =
    t.write_promise
    >>= function
    | Error exn ->
        Log.err (fun m -> m "Error on write promise %a" Fmt.exn exn) ;
        Lwt.return_unit
    | Ok () -> (
        Lwt.catch
          (fun () -> Lwt_io.close t.channel >>= Lwt.return_ok)
          Lwt.return_error
        >>= function
        | Ok () ->
            Log.info (fun m -> m "Closed wal successfully") ;
            Lwt.return_unit
        | Error exn ->
            Log.err (fun m -> m "Failed to close wal with %a" Fmt.exn exn) ;
            Lwt.return_unit )
end
