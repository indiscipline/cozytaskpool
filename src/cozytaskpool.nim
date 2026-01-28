import std/[tasks, osproc, options]
import pkg/threading/channels

export tasks

when not (defined(gcArc) or defined(gcOrc) or defined(gcAtomicArc) or defined(nimdoc)):
  {.error: "This package requires one of --mm:arc / --mm:atomicArc / --mm:orc compilation flags".}
when not compileOption("threads"):
  {.error: "This package requires --threads:on compilation flag".}

type
  RunnerArgs = tuple[tasks: Chan[Task], results: Option[Chan[Task]]]
  ConsumerArgs = tuple[results: Chan[Task], nthreads: Positive]
  CozyTaskPool* = object
    nthreads: Positive = 1
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
    args.tasks.recv(t)
    try: t.invoke()
    except StopFlag: break
  if args.results.isSome():
    (args.results.unsafeGet()).send(toTask(stop())) # notify consumer thread finished

proc consumer(args: ConsumerArgs) {.thread.} =
  var activethreads: Natural = args.nthreads
  var t: Task
  while activethreads > 0:
    args.results.recv(t)
    try: t.invoke()
    except StopFlag: dec(activethreads)

func resultsChan*(pool: CozyTaskPool): Chan[Task] {.inline raises:[UnpackDefect].} =
  ## Assumes the pool was created with the Consumer thread.
  ## If not, will raise an UnpackDefect exception.
  assert pool.results.isSome()
  pool.results.get()

func resultsAddr*(pool: CozyTaskPool): ptr Chan[Task] {.inline raises:[UnpackDefect] deprecated: "use resultsChan() instead".} =
  assert pool.results.isSome()
  pool.results.get().unsafeAddr()

template consume*(results: Chan[Task]; consumer: typed{nkCall}) =
  ## Helper template to wrap a call in a `tasks.toTask` macro
  results.send(toTask(consumer))

proc sendTask*(pool: var CozyTaskPool; task: sink Task) {.inline.} =
  ## Send a task to the pool.
  ## For procedure calls, use with `tasks.toTask` macro:
  ## `pool.sendTask(toTask(foo(bar)))`
  pool.tasks.send(isolate(task))

template sendTask*(pool: var CozyTaskPool; worker: typed{nkCall}) =
  ## Helper template to wrap a call in a `tasks.toTask` macro
  ## `pool.sendTask(foo(bar))`
  pool.sendTask(toTask(worker))

proc newTaskPool*(nthreads: Positive = countProcessors(); createConsumer: bool = true): CozyTaskPool {.noinit.} =
  ## Creates the pool and launches its threads, awaiting tasks to execute.
  var pool: CozyTaskPool
  pool.nthreads = nthreads
  pool.taskThreads = newSeq[Thread[RunnerArgs]](nthreads)
  pool.tasks = newChan[Task]()
  if createConsumer:
    pool.results = some(newChan[Task]())
    createThread(pool.consumerThread, consumer, (pool.results.get(), nthreads))
  else:
    pool.results = none(Chan[Task])
  let resultsOpt = if pool.results.isSome(): some(pool.results.get()) else: none(Chan[Task])
  for ti in 0..high(pool.taskThreads):
    createThread(pool.taskThreads[ti], runner, (pool.tasks, resultsOpt))
  pool

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

      proc work(consumer: Chan[Task]; inputData: int) =
        sleep(100)
        let r = inputData - 1
        consumer.send(toTask( log(r) ))

      for x in data:
        pool.sendTask(toTask( work(pool.resultsChan(), x) ))

      pool.stopPool()
      check results == checkset
