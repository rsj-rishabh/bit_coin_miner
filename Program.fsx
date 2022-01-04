#r "nuget: Akka.FSharp" 
#r "nuget: Akka.Remote"
#r "nuget: Akka.Serialization.Hyperion"
#time "on"

open Akka.FSharp
open Akka.Actor
open System.Text
open System.Security
open System.Diagnostics
open System.Net
open System.Net.NetworkInformation
open System.Net.Sockets

// Enumerations
// State of supervisor actor
type SupervisorState = {
    Actors: Set<IActorRef>
    CurrentStartLimit: int
    CurrentEndLimit: int
    Increment: int
    NumberOfSolutionsFound: int
}

// State of worker actor
type WorkerState = {
    LeadingZeroes: int
    InputString: string
}

// Enumeration for messages sent between Supervisor-worker
type Message =
    | Join of bool
    | Stop of bool
    | Completed of string * string
    | Input of int * string
    | GiveWork of bool
    | Range of int * int

let localIPs () =
    let networkInterfaces = NetworkInterface.GetAllNetworkInterfaces()
                            |> Array.filter(fun iface -> iface.OperationalStatus.Equals(OperationalStatus.Up))

    let addresses = seq {
        for iface in networkInterfaces do
            for unicastAddr in iface.GetIPProperties().UnicastAddresses do
                yield unicastAddr.Address}

    addresses
    |> Seq.filter(fun addr -> addr.AddressFamily.Equals(AddressFamily.InterNetwork))
    |> Seq.filter(IPAddress.IsLoopback >> not)
    |> Seq.head

// Node configuration
let configurationString (port:string)= 
    let str1 = @"akka {
            actor {
                provider = ""Akka.Remote.RemoteActorRefProvider, Akka.Remote""
                debug : {
                    receive : on
                    autoreceive : on
                    lifecycle : on
                    event-stream : on
                    unhandled : on
                }
                serializers {
                    hyperion = ""Akka.Serialization.HyperionSerializer, Akka.Serialization.Hyperion""
                }
                serialization-bindings {
                    ""System.Object"" = hyperion
                }
            }
            remote {
                helios.tcp {
                    port = "

    let str2 = @"
                    hostname ="
    let str3 = @"
            }
        }
    }"
    Configuration.parse (str1+port+str2+localIPs().ToString()+str3)

// Port Numbers for Server, client
let serverPort = "9903"
let clientPort = "9910"

// Number of Solutions to find
let numberOfSolutionsFound= 50


// Just for calculating time
let proc = Process.GetCurrentProcess()
let cpuTimeStamp = proc.TotalProcessorTime
let timer = Stopwatch()
timer.Start()

let completed stringWithNonce hash = 
    printfn "One of the workers has solved the problem."
    printfn "OUTPUT: %s %s" stringWithNonce hash

// Converts byte array to hex string
let byteToHex (bytes:byte[]) =
        (StringBuilder(), bytes)
        ||> Array.fold (fun state -> sprintf "%02X" >> state.Append)  
        |> string

// Converts byte array to hex string
let stop(printTime: bool, numSol: int,  systemRef:ActorSystem) =
//let stop(printTime: bool, slowDown: bool) =    
    if printTime then
        let cpuTime = (proc.TotalProcessorTime-cpuTimeStamp).TotalMilliseconds
        printfn "CPU time = %dms" (int64 cpuTime)
        printfn "Absolute time = %dms" timer.ElapsedMilliseconds
    if numSol = numberOfSolutionsFound then
        systemRef.Terminate()
        |> ignore

// Supervisor Actor Definition
// A Supervisor Actor is for keeping track of Worker Actors and their range of subproblems.
let supervisor (inputState: Message,  systemRef:ActorSystem) (mailbox:Actor<_>) =

    let rec loop state = actor {
        let! message = mailbox.Receive()
        let sender = mailbox.Sender()

        match message with
        | Join(_) -> 
            // ** printfn "Join request received."
            // ** printfn "Number of workers: %d " (state.Actors.Count+1)
            sender <! inputState
            return! loop { state with Actors = Set.add sender state.Actors }
        | GiveWork(_) -> 
            // ** printfn "Work request received. Work range sent."
            sender <! Range(state.CurrentStartLimit,state.CurrentEndLimit)
            // worker1 gets 1-100000, worker2 gets 100001-200000, if worker1 comes back, he gets 200001-300000, and so on.
            return! loop {state with CurrentStartLimit = state.CurrentStartLimit + state.Increment;CurrentEndLimit = state.CurrentEndLimit+ state.Increment}
        | Completed(stringWithNonce, hash) -> 
            completed stringWithNonce hash
            if state.NumberOfSolutionsFound+1= numberOfSolutionsFound
            then
                printfn "Stopping all actors and their nodes."
                Set.iter (fun x -> x <! Stop(true)) state.Actors
                printfn "Stopped all actors and their nodes."
            stop (true, state.NumberOfSolutionsFound+1, mailbox.Context.System)
            // stop (true, false)
            return! loop {state with NumberOfSolutionsFound=state.NumberOfSolutionsFound+1}
            //return()
        | _ ->  failwith "Unknown message "
    }
    
    loop { Actors = Set.empty; CurrentStartLimit = 1; CurrentEndLimit = 1000000; Increment = 1000000; NumberOfSolutionsFound=0}


// Worker Actor Definition
// Worker actor is responsible for mining.
let worker (ipAddress:string, systemRef:ActorSystem) (mailbox:Actor<_>) =

    let serveRef = systemRef.ActorSelection($"akka.tcp://Server@{ipAddress}/user/supervisor")
    // ** printfn "Join request sent."
    serveRef <! Join(true)

    let rec loop state = actor {
        let! message = mailbox.Receive()
        let sender = mailbox.Sender()

        match message with
        | Input(leadingZeroesSupervisor, inputStringSupervisor) ->
            // ** printfn "Join requested accepted, inputs received."
            // ** printfn "LeadingZeroes: %d, InputString: %s " leadingZeroesSupervisor inputStringSupervisor
            // ** printfn "Requesting supervisor for work."
            sender <! GiveWork(true)
            return! loop ({LeadingZeroes = leadingZeroesSupervisor; InputString = inputStringSupervisor})
        | Range(startL, endL) ->
            // ** printfn "Work received. Start: %d, End: %d . Working on this. Please be patient." startL endL
            let mutable stringToHash: string = ""
            let mutable hash: string = ""
            let findNonce = 
                let mutable nonce: int = startL-1
                let mutable result: bool = false
                let mutable zeroesString = ""
                for i in 1..state.LeadingZeroes do
                    zeroesString <- zeroesString + "0"
                while nonce<endL && not result do
                    nonce <- nonce + 1
                    stringToHash <- state.InputString + string(nonce)
                    let bytes = Encoding.ASCII.GetBytes stringToHash
                    let mySHA256 = Cryptography.SHA256.Create()
                    let hashByteArray = mySHA256.ComputeHash bytes
                    let finalHash = byteToHex hashByteArray
                    let testseq =  finalHash.[..state.LeadingZeroes-1]
                    result <- testseq = zeroesString
                    if result then hash<-finalHash else hash<-""
                if result then nonce else -1
            if findNonce <> -1 then 
                sender <! Completed(stringToHash,hash)
                sender <! GiveWork(true)
                printfn "%s found solution at: %d" mailbox.Self.Path.Name findNonce
            else sender <! GiveWork(true)
                 // ** printfn "No solution found between %d and %d. Requesting supervisor for more work." startL endL
            return! loop state
        | Stop(_) -> 
            printfn "Worker stopping."
            stop (false, numberOfSolutionsFound,  mailbox.Context.System)
            //stop (false, false)
            return()
        | _ ->  failwith "Unknown message "
    }

    loop {LeadingZeroes =  0; InputString = ""}


let start (args: string[]) =
    if System.Int32.TryParse args.[1] |> fst
    then
        // ** printfn "This is a server! "
        let systemRef = System.create "Server" (configurationString serverPort)

        let inputState = Input(int(args.[1]), string(args.[2])) //Arg1 is leading zeroes required in hash, Arg2 is input string
        spawn systemRef "supervisor" (supervisor(inputState, systemRef)) 
        |> ignore
        let serverIpWithPort = localIPs().ToString()+":"+serverPort
        for i in 1..12 do
            spawn systemRef ("workera"+string(i)) (worker(serverIpWithPort, systemRef))
            |> ignore
        systemRef.WhenTerminated.Wait()
    else
        // ** printfn "This is a client! "
        let systemRef = System.create "Client" (configurationString clientPort)
        for i in 1..6 do
            spawn systemRef ("workerb"+string(i)) (worker(string(args.[1]), systemRef)) //Arg1 is Server IP Address with port number
            |> ignore 
        systemRef.WhenTerminated.Wait()

// Code starts here
start(fsi.CommandLineArgs)
