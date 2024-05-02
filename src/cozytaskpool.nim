import std/[tasks, osproc, macros, options], threading/channels

export tasks

when not (defined(gcArc) or defined(gcOrc) or defined(gcAtomicArc) or defined(nimdoc)):
  {.error: "This package requires one of --mm:arc / --mm:atomicArc / --mm:orc compilation flags".}
when not compileOption("threads"):
  {.error: "This package requires --threads:on compilation flag".}

type
  RunnerArgs = tuple[tasks: ptr Chan[Task], results: Option[ptr Chan[Task]]]
  ConsumerArgs = tuple[results: ptr Chan[Task], nthreads: Positive]
  CozyTaskPool* = object
    nthreads: Positive
    taskThreads: seq[Thread[RunnerArgs]]
    consumerThread: Thread[ConsumerArgs] ## |
    ## Can be nil, if the pool was created with `createConsumer = false`
    tasks: Chan[Task]
    results: Option[Chan[Task]]
  StopFlag = object of CatchableError

proc `=copy`(dest: var CozyTaskPool; source: CozyTaskPool) {.error.}

proc stop() = raise newException(StopFlag, "")

proc runner(args: RunnerArgs) {.thread.} =
  var t: Task
  while true:
    args.tasks[].recv(t)
    try: t.invoke()
    except StopFlag: break
  if args.results.isSome():
    (args.results.get())[].send(toTask(stop())) # notify consumer thread finished

proc consumer(args: ConsumerArgs) {.thread.} =
  var activethreads: Natural = args.nthreads
  var t: Task
  while activethreads > 0:
    args.results[].recv(t)
    try: t.invoke()
    except StopFlag: dec(activethreads)

func resultsAddr*(pool: CozyTaskPool): ptr Chan[Task] {.inline raises:[UnpackDefect].} =
  ## Assumes the pool was created with the Consumer thread.
  ## If not, will raise an UnpackDefect exception.
  assert pool.results.isSome()
  pool.results.get().unsafeAddr

template consume*(results: ptr Chan[Task]; consumer: typed{nkCall}) =
  ## Helper template to wrap a call in a `tasks.toTask` macro
  results[].send(toTask(consumer))

proc sendTask*(pool: var CozyTaskPool; task: sink Task) {.inline.} =
  ## Send a task to the pool.
  ## For procedure calls, use with `tasks.toTask` macro:
  ## `pool.sendTask(toTask(foo(bar)))`
  pool.tasks.send(isolate(task))

template sendTask*(pool: var CozyTaskPool; worker: typed{nkCall}) =
  ## Helper template to wrap a call in a `tasks.toTask` macro
  ## `pool.sendTask(foo(bar))`
  pool.sendTask(toTask(worker))

proc newTaskPool*(nthreads: Positive = countProcessors(); createConsumer: bool = true): CozyTaskPool =
  ## Creates the pool and launches its threads, awaiting tasks to execute.
  result.nthreads = nthreads
  result.taskThreads = newSeq[Thread[RunnerArgs]](nthreads)
  result.tasks = newChan[Task]()
  if createConsumer:
    result.results = some(newChan[Task]())
    createThread(result.consumerThread, consumer, (result.results.get().addr, nthreads))
  else:
    result.results = none(Chan[Task])
  let resultsOpt = if result.results.isSome(): some(result.results.get().addr) else: none(ptr Chan[Task])
  for ti in 0..high(result.taskThreads):
    createThread(result.taskThreads[ti], runner, (result.tasks.addr, resultsOpt))
  result

proc stopPool*(pool: var CozyTaskPool) =
  ## Sends the stopping message to the worker threads and blocks till completion
  for _ in pool.taskThreads: pool.tasks.send(toTask(stop()))
  joinThreads(pool.taskThreads)
  if pool.results.isSome():
    joinThread(pool.consumerThread)

when isMainModule:
  import std/[os, unittest]

  var
    data = [2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61]
    checkset: set[byte] = {1.byte, 2, 4, 6, 10, 12, 16, 18, 22, 28, 30, 36, 40, 42, 46, 52, 58, 60}
    results: set[byte]

  suite "Cozy Task Pool test suite":
    setup:
      var pool: CozyTaskPool = newTaskPool()

    test "Test completion":
      proc log(inputData: int) =
        results.incl(inputData.byte)
        # echo "Received some message about ", inputData

      proc work(consumer: ptr Chan[Task]; inputData: int) =
        sleep(100)
        let r = inputData - 1
        consumer[].send(toTask( log(r) ))

      for x in data:
        pool.sendTask(toTask( work(pool.resultsAddr(), x) ))

      pool.stopPool()
      check results == checkset
