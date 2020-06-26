open Lwt.Infix
open Owal

let src = Logs.Src.create "Bench"

module Log = (val Logs.src_log src : Logs.LOG)

module T_p = struct
  type t = int list

  let init () = []

  type op = Write of int

  let encode_blit = function
    | Write i ->
        ( 8
        , fun buf ~offset ->
            EndianBytes.LittleEndian.set_int64 buf offset (Int64.of_int i) )

  let decode buf ~offset =
    Write (EndianBytes.LittleEndian.get_int64 buf offset |> Int64.to_int)

  let apply t (Write i) = i :: t
end

module T = Persistant (T_p)

let time_it f =
  let start = Unix.gettimeofday () in
  f () >>= fun () -> Unix.gettimeofday () -. start |> Lwt.return

let test_file = "test.tmp"

let throughput n () =
  Log.info (fun m -> m "Setting up throughput test") ;
  T.of_file test_file
  >>= fun t ->
  let ops = List.init n (fun _ -> Random.int 100000) in
  let test () =
    List.fold_left (fun t v -> T.change (T_p.Write v) t) t ops
    |> T.sync |> Lwt_result.get_exn
  in
  Log.info (fun m -> m "Starting throughput test") ;
  time_it test
  >>= fun time ->
  Log.info (fun m -> m "Closing wal") ;
  T.close t
  >>= fun () ->
  Log.info (fun m -> m "Finished throughput test!") ;
  Fmt.str "Took %f to do %d operations: %f ops/s" time n
    Core.Float.(of_int n / time)
  |> Lwt.return

let reporter =
  let open Core in
  let report src level ~over k msgf =
    let k _ = over () ; k () in
    let src = Logs.Src.name src in
    msgf
    @@ fun ?header ?tags:_ fmt ->
    Fmt.kpf k Fmt.stdout
      ("[%a] %a %a @[" ^^ fmt ^^ "@]@.")
      Time.pp (Time.now ())
      Fmt.(styled `Magenta string)
      (Printf.sprintf "%14s" src)
      Logs_fmt.pp_header (level, header)
  in
  {Logs.report}

let () =
  Logs.(set_level (Some Info)) ;
  Logs.set_reporter reporter ;
  let res = Lwt_main.run @@ throughput 100000 () in
  Unix.unlink test_file ; print_endline res
