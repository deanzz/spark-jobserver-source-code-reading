# spark-jobserver源码解读
>spark-jobserver是咱们elemental系统中一个重要的组件，但是它存在很多问题，比如由于jobserver和aco两边状态的不一致，导致的context同时被多个任务使用；
jobserver UI上显示的任务状态不对，需要手动维护正确的状态等问题等问题。<br/>
为了接下来改进jobserver，最近阅读了部分源码，做好修改的准备。

这次分享主要是解读`服务的启动`、`创建context`和`提交job`三个部分的源码，并且只解读`正常分支`。<br/>
代码是master分支截至2017.11.27提交的版本（目前最新）。<br/>
建议大家clone下来最新的master分支代码，将11.27的提交新建一个分支，跟着下面的步骤，一步一步跟踪，这样你会理解的更快。

## 服务的启动
1. server_start.sh<br/>
首先要找到启动的入口，大家平时重启jobserver的时候，用的是什么？对，就是server_start.sh，他就是启动的入口，从它开始看，<br/>
重点就是下面两个部分，MAIN定义了启动的入口类，然后使用spark-submit命令调用spark.jobserver.JobServer中的main方法，启动服务。
```bash
...
MAIN="spark.jobserver.JobServer"
...
cmd='$SPARK_HOME/bin/spark-submit --class $MAIN --driver-memory $JOBSERVER_MEMORY
  --conf "spark.executor.extraJavaOptions=$LOGGING_OPTS"
  --driver-java-options "$GC_OPTS $JAVA_OPTS $LOGGING_OPTS $CONFIG_OVERRIDES"
  $@ $appdir/spark-job-server.jar $conffile'
if [ -z "$JOBSERVER_FG" ]; then
  eval $cmd > $LOG_DIR/server_start.log 2>&1 < /dev/null &
  echo $! > $PIDFILE
else
  eval $cmd
fi
```

2. JobServer.start<br/>
接下来的线索是什么？对，就是上面定义的入口类，即MAIN变量的值，我们来看看JobServer这个类，<br/>
这个类中有main方法，main方法中调用start方法，所以咱们主要看start方法的逻辑。<br/>
start方法的前半部分主要对各种配置的合法性进行了判断，后半部分是重点<br/>
首先是一些actor的初始化，其中supervisor的初始化最为关键，注意看下面注释。
```scala
// 读写数据库的dao层actor，用于管理jobserver的内部状态
val daoActor = system.actorOf(Props(classOf[JobDAOActor], jobDAO), "dao-manager")
// 管理本地数据文件的actor，被DataRoutes中数据文件操作接口调用，这里存的数据可以在任务中使用
val dataManager = system.actorOf(Props(classOf[DataManagerActor], dataFileDAO), "data-manager")
// 管理jar文件的actor
val binManager = system.actorOf(Props(classOf[BinaryManager], daoActor), "binary-manager")
// 监督者actor，用于管理context即对应的jobManager，
// 比如创建context、删除context，其中AkkaClusterSupervisorActor是集群版本的，LocalContextSupervisorActor是本地进程内版本的
val supervisor =
  system.actorOf(Props(
    if (contextPerJvm) {
      classOf[AkkaClusterSupervisorActor]
    } else {
      classOf[LocalContextSupervisorActor]
    },
    daoActor, dataManager), "context-supervisor")
// 管理job状态的actor，被jobRoutes中的接口调用
val jobInfo = system.actorOf(Props(classOf[JobInfoActor], jobDAO, supervisor), "job-info")
```

接着根据配置文件（若配置），存储需要的jar文件。    
```scala
// Add initial job JARs, if specified in configuration.
storeInitialBinaries(config, binManager)
```

接着根据配置文件（若配置），新增context。
```scala
// Create initial contexts
supervisor ! ContextSupervisor.AddContextsFromConfig
```

最最重要的重点来了，就是启动API服务，服务启动成功后，就可以接收请求了。<br/>
WebApi中包含很多路由配置，在这里你可以根据uri和http method找到感兴趣的API的描述和处理逻辑。
```scala
new WebApi(system, config, port, binManager, dataManager, supervisor, jobInfo).start()
```

3. WebApi.start
JobServer的start方法调用了WebApi.start方法，<br/>
WebService.start是spray内部的方法，将各种路由加到服务中，并启动了服务，绑定了监听地址和端口。<br/>
再想了解细节，可以去学习一下spray框架，但达到明白jobserver服务启动的流程，知道这个方法是启动web服务的，就已经足够了。
```scala
def start() {
    ...
    logger.info("Starting browser web service...")
    WebService.start(myRoutes ~ commonRoutes, system, bindAddress, port)
  }
```

至此，我们的jobserver就启动成功了。

## 创建context
1. WebApi.contextRoutes<br/>
首先我们还是要找到创建context的入口，入口主要有2个，<br/>
a.上面提到的通过配置添加context，``supervisor ! ContextSupervisor.AddContextsFromConfig`` <br/>
b.通过创建context的API添加context，即API (POST /contexts/{contextName}?{optional params})<br/>
我们通过第二个入口`b`来了解创建context的过程。<br/>
这里的重点是`发送创建context的消息给监督者actor的部分`和`对返回结果的处理部分`
```scala
post {
  /**
   *  POST /contexts/<contextName>?<optional params> -
   *    Creates a long-running context with contextName and options for context creation
   *    All options are merged into the defaults in spark.context-settings
   *
   * @optional @entity The POST entity should be a Typesafe Config format file with a
   *            "spark.context-settings" block containing spark configs for the context.
   * @optional @param num-cpu-cores Int - Number of cores the context will use
   * @optional @param memory-per-node String - -Xmx style string (512m, 1g, etc)
   * for max memory per node
   * @return the string "OK", or error if context exists or could not be initialized
   */
  entity(as[String]) { configString =>
    path(Segment) { (contextName) =>
      ...
      // 发送创建context的消息给监督者actor
      val future = (supervisor ? AddContext(cName, config))(contextTimeout.seconds)
      // 处理返回
      respondWithMediaType(MediaTypes.`application/json`) { ctx =>
        future.map {
          case ContextInitialized => ctx.complete(StatusCodes.OK,
            successMap("Context initialized"))
          case ContextAlreadyExists => badRequest(ctx, "context " + contextName + " exists")
          case ContextInitError(e) => ctx.complete(500, errMap(e, "CONTEXT INIT ERROR"))
        }
      }
      ...
    }
  }
}
```

2. AkkaClusterSupervisorActor.AddContext<br/>
接下来当然是查看AddContext消息的处理逻辑，这里只针对集群模式，因为这是我们一直在用的。<br/>
AddContext消息的处理逻辑中关键就是startContext方法的调用和其返回的处理。
```scala
case AddContext(name, contextConfig) =>
   ...
   startContext(name, mergedConfig, false) { ref =>
     originator ! ContextInitialized
   } { err =>
     originator ! ContextInitError(err)
   }
   ...
```

3. AkkaClusterSupervisorActor.startContext<br/>
接下来看看startContext的逻辑，可以看出，context创建逻辑的精髓就在这里了。<br/>
方法前半部分是准备参数，最后将参数传给创建进程的命令，创建context子进程。
```scala
...
// 根据命令创建进程
// 这里，如果context-per-jvm配置为true，则使用的是jobserver自带的manager_start.sh脚本
// 或者通过配置deploy.manager-start-cmd，获取自定义的脚本
// 咱们的情况使用的是manager_start.sh脚本
val process = Process(managerStartCommand, managerArgs)
// 运行进程
process.run(ProcessLogger(out => contextLogger.info(out), err => contextLogger.warn(err)))
...
```

4. manager_start.sh<br/>
接下来该看哪了？进程是通过manager_start.sh脚本创建的，所以咱们来看看manager_start.sh。<br/>
套路还是一样，使用spark-submit命令调用MAIN定义的启动类`spark.jobserver.JobManager`中的main方法，启动context，即jobManager。
```bash
...
MAIN="spark.jobserver.JobManager"
...
cmd='$SPARK_HOME/bin/spark-submit --class $MAIN --driver-memory $JOBSERVER_MEMORY
      --conf "spark.executor.extraJavaOptions=$LOGGING_OPTS"
      $SPARK_SUBMIT_OPTIONS
      --driver-java-options "$GC_OPTS $JAVA_OPTS $LOGGING_OPTS $CONFIG_OVERRIDES $SPARK_SUBMIT_JAVA_OPTIONS"
      $JAR_FILE $3 $4 $CONF_FILE'

eval $cmd 2>&1 > $5/spark-job-server.out
```

5. JobManager<br/>
接下来看看JobManager类，套路一样，main方法中调用start方法，重点看start方法。<br/>
start方法中前半部分就是获取和整合配置，并初始化一些actor，接着重点来了，它会将子进程中的jobManagerActor加入AkkaClusterSupervisorActor所在的集群。<br/>
```scala
...
// 启动JobManagerActor
val jobManager = system.actorOf(JobManagerActor.props(daoActor), managerName)
// 将JobManagerActor所在的ActorSystem加入到AkkaClusterSupervisorActor所在的集群
logger.info("Joining cluster at address {}", clusterAddress)
Cluster(system).join(clusterAddress)
...
```

6. AkkaClusterSupervisorActor.MemberUp<br/>
接下来看哪里？是不是有点儿蒙圈了？的确，如果你要对akka-cluster没有基本的认识，真的就找不到下一步的线索了。<br/>
所以要接着往下看，你还要有一些akka-cluster的知识储备。<br/>
akka-cluster中，如果你订阅了MemberEvent，则在一个新节点加入集群时，新节点的状态会变为Up，集群会接收到MemberUp的消息。<br/>
所以接下来就要看AkkaClusterSupervisorActor中MemberUp的消息处理了。<br/>
```scala
case MemberUp(member) =>
  //只处理集群中role为manager的member
  if (member.hasRole("manager")) {
    val memberActors = RootActorPath(member.address) / "user" / "*"
    // 向member所在system下的所有用户创建的actor发送Identify消息
    // 目的是为了获取集群节点中JobManagerActor的ActorRef
    // Identify是一个actor都认识的内部消息，其返回的消息为ActorIdentity
    context.actorSelection(memberActors) ! Identify(memberActors)
  }
```

7. AkkaClusterSupervisorActor.ActorIdentity<br/>
大家找到下一步的线索了吗？如果没找到，注意看上面第6步中代码的注释。接着来看ActorIdentity消息的处理。<br/>
该消息处理中，核心逻辑就是initContext方法。
```scala
case ActorIdentity(memberActors, actorRefOpt) =>
    ...
    initContext(contextConfig, actorName,
                actorRef, contextInitTimeout)(isAdHoc, successFunc, failureFunc)
    ...
```

8. AkkaClusterSupervisorActor.initContext<br/>
该方法中会给新加入的节点的JobManagerActor发送Initialize初始化的消息，进行JobManager的初始化。<br/>
最终根据返回类型做出相应的反馈。
```scala
(ref ? JobManagerActor.Initialize(
    contextConfig, Some(resultActor), dataManagerActor))(Timeout(timeoutSecs.second)).onComplete {
    case Failure(e: Exception) =>
      logger.info("Failed to send initialize message to context " + ref, e)
      cluster.down(ref.path.address)
      ref ! PoisonPill
      failureFunc(e)
    case Success(JobManagerActor.InitError(t)) =>
      logger.info("Failed to initialize context " + ref, t)
      cluster.down(ref.path.address)
      ref ! PoisonPill
      failureFunc(t)
    case Success(JobManagerActor.Initialized(ctxName, resActor)) =>
      logger.info("SparkContext {} joined", ctxName)
      contexts(ctxName) = (ref, resActor)
      context.watch(ref)
      successFunc(ref)
    case _ => logger.info("Failed for unknown reason.")
      cluster.down(ref.path.address)
      ref ! PoisonPill
      failureFunc(new RuntimeException("Failed for unknown reason."))
  }
```

9. JobManagerActor.Initialize<br/>
JobManagerActor的初始化消息中，核心逻辑就是开始真正的创建SparkContext
```scala
case Initialize(ctxConfig, resOpt, dataManagerActor) =>
    ...
    factory = getContextFactory()
    // 开始真正的创建SparkContext
    jobContext = factory.makeContext(config, contextConfig, contextName)
    ...
```

10. SparkContextFactory.makeContext<br/>
创建context时，首先是调用configToSparkConf方法，根据所有配置，生成SparkConf，<br/>
然后调用DefaultSparkContextFactory.makeContext方法创建SparkContext。
```scala
def makeContext(config: Config, contextConfig: Config, contextName: String): C = {
    // configToSparkConf方法中，将配置设置到SparkConf，比如context的内存、cpu核数等
    val sparkConf = configToSparkConf(config, contextConfig, contextName)
    makeContext(sparkConf, contextConfig, contextName)
  }
```

11. DefaultSparkContextFactory.makeContext<br/>
重点就是``new SparkContext(sparkConf)``，根据sparkConf实例化SparkContext。
```scala
def makeContext(sparkConf: SparkConf, config: Config, contextName: String): C = {
    val sc = new SparkContext(sparkConf) with ContextLike {
      def sparkContext: SparkContext = this
    }
    for ((k, v) <- SparkJobUtils.getHadoopConfig(config)) sc.hadoopConfiguration.set(k, v)
    sc
  }
```

12. AkkaClusterSupervisorActor.initContext<br/>
SparkContext创建成功后，返回Initialized消息，还记得第8步中对初始化JobManagerActor的返回处理吗？<br/>
在这里接收到Initialized消息，并处理。
```scala
case Success(JobManagerActor.Initialized(ctxName, resActor)) =>
        logger.info("SparkContext {} joined", ctxName)
        contexts(ctxName) = (ref, resActor)
        context.watch(ref)
        successFunc(ref)
```

13. AkkaClusterSupervisorActor.AddContext<br/>
根据上面第12步，最后一行``successFunc(ref)``，继续向上层返回结果，还记得上面第2步中对startContext方法结果的处理逻辑吗？<br/>
它将返回ContextInitialized消息
```scala
originator ! ContextInitialized
```

14. WebApi.contextRoutes<br/>
还记得第1步中对`对返回结果的处理部分`吗？在这里，向客户端返回200 OK的http状态，并返回context成功初始化的Json。
```scala
case ContextInitialized => ctx.complete(StatusCodes.OK,
                      successMap("Context initialized"))
```

至此，一个context就被成功创建了。

## 提交job
1. WebApi.jobRoutes<br/>
首先还是找到提交job的入口，即API (POST /jobs)，其中的核心逻辑是<br/>
a.通过contextName找到相应的jobManager<br/>
b.向jobManager发送StartJob消息<br/>
c.处理StartJob消息的返回<br/>
```scala
/**
 * POST /jobs   -- Starts a new job.  The job JAR must have been previously uploaded, and
 *                 the classpath must refer to an object that implements SparkJob.  The `validate()`
 *                 API will be invoked before `runJob`.
 *
 * @entity         The POST entity should be a Typesafe Config format file;
 *                 It will be merged with the job server's config file at startup.
 * @required @param appName String - the appName for the job JAR
 * @required @param classPath String - the fully qualified class path for the job
 * @optional @param context String - the name of the context to run the job under.  If not specified,
 *                                   then a temporary context is allocated for the job
 * @optional @param sync Boolean if "true", then wait for and return results, otherwise return job Id
 * @optional @param timeout Int - the number of seconds to wait for sync results to come back
 * @return JSON result of { StatusKey -> "OK" | "ERROR", ResultKey -> "result"}, where "result" is
 *         either the job id, or a result
 */
post {
  entity(as[String]) { configString =>
    parameters('appName, 'classPath,
      'context ?, 'sync.as[Boolean] ?, 'timeout.as[Int] ?, SparkJobUtils.SPARK_PROXY_USER_PARAM ?) {
        (appName, classPath, contextOpt, syncOpt, timeoutOpt, sparkProxyUser) =>
          try {
            ...
            // 通过contextName找到相应的jobManager
            val jobManager = getJobManagerForContext(
              contextOpt.map(_ => cName), cConfig, classPath)
            ...
            //向jobManager发送StartJob消息
            val future = jobManager.get.ask(
              JobManagerActor.StartJob(appName, classPath, jobConfig, events))(timeout)
            // 处理StartJob消息的返回
            respondWithMediaType(MediaTypes.`application/json`) { ctx =>
              future.map {
                case JobResult(jobId, res) =>
                  res match {
                    case s: Stream[_] => sendStreamingResponse(ctx, ResultChunkSize,
                      resultToByteIterator(Map.empty, s.toIterator))
                    case _ => ctx.complete(Map[String, Any]("jobId" -> jobId) ++ resultToTable(res))
                  }
                case JobErroredOut(jobId, _, ex) => ctx.complete(
                  Map[String, String]("jobId" -> jobId) ++ errMap(ex, "ERROR")
                )
                case JobStarted(_, jobInfo) =>
                  val future = jobInfoActor ? StoreJobConfig(jobInfo.jobId, postedJobConfig)
                  future.map {
                    case JobConfigStored =>
                      ctx.complete(202, getJobReport(jobInfo, jobStarted = true))
                  }.recover {
                    case e: Exception => ctx.complete(500, errMap(e, "ERROR"))
                  }
                case JobValidationFailed(_, _, ex) =>
                  ctx.complete(400, errMap(ex, "VALIDATION FAILED"))
                case NoSuchApplication => notFound(ctx, "appName " + appName + " not found")
                case NoSuchClass => notFound(ctx, "classPath " + classPath + " not found")
                case WrongJobType =>
                  ctx.complete(400, errMap("Invalid job type for this context"))
                case JobLoadingError(err) =>
                  ctx.complete(500, errMap(err, "JOB LOADING FAILED"))
                case NoJobSlotsAvailable(maxJobSlots) =>
                  val errorMsg = "Too many running jobs (" + maxJobSlots.toString +
                    ") for job context '" + contextOpt.getOrElse("ad-hoc") + "'"
                  ctx.complete(503, Map(StatusKey -> "NO SLOTS AVAILABLE", ResultKey -> errorMsg))
                case ContextInitError(e) => ctx.complete(500, errMap(e, "CONTEXT INIT FAILED"))
              }.recover {
                case e: Exception => ctx.complete(500, errMap(e, "ERROR"))
              }
            }
          } catch {
            case e: NoSuchElementException =>
              complete(StatusCodes.NotFound, errMap("context " + contextOpt.get + " not found"))
            case e: ConfigException =>
              complete(StatusCodes.BadRequest, errMap("Cannot parse config: " + e.getMessage))
            case e: Exception =>
              complete(500, errMap(e, "ERROR"))
          }
      }
  }
}
```

2. JobManagerActor.StartJob
接着就要看StartJob的消息处理，其中的核心逻辑是调用了startJobInternal方法
```scala
case StartJob(appName, classPath, jobConfig, events) => {
    ...
    startJobInternal(appName, classPath, jobConfig, events, jobContext, sparkEnv)
  }
```

3. JobManagerActor.startJobInternal
该方法的核心处理逻辑是先加载当前任务，即根据任务的classPath实例化SparkJob，<br/>
然后将当前的任务及JobManagerActor注册到结果处理actor resultActor和状态处理actor statusActor中，<br/>
最后调用getJobFuture方法，获得任务执行的future。
```scala
...
val jobContainer = factory.loadAndValidateJob(appName, lastUploadTime,
                                                  classPath, jobCache)
...
// Automatically subscribe the sender to events so it starts getting them right away
resultActor ! Subscribe(jobId, sender, events)
statusActor ! Subscribe(jobId, sender, events)  
...
Some(getJobFuture(jobContainer, jobInfo, jobConfig, sender, jobContext, sparkEnv))
```

4. JobManagerActor.getJobFuture
下面来看看getJobFuture方法，这里就是跑任务的核心逻辑了。<br/>
先获取已加载的job，然后根据用户实现的子类中的验证方法验证job的合法性，再返回任务成功提交的状态给前端，<br/>
最后调用用户实现子类中的runJob方法，执行任务，这里是在返回提交成功结果后，后台继续执行。<br/>
待任务完成后，就会进入andThen，分别发送任务结果和任务状态给resultActor和statusActor。
```scala
Future {  
  ...
  try {
    ...
    // 获取已加载的job
    val job = container.getSparkJob
    try {
      statusActor ! JobStatusActor.JobInit(jobInfo)
      val jobC = jobContext.asInstanceOf[job.C]
      val jobEnv = getEnvironment(jobId)
      // 根据用户实现的子类中的验证方法验证job是否合法
      job.validate(jobC, jobEnv, jobConfig) match {
        case Bad(reasons) =>
          val err = new Throwable(reasons.toString)
          statusActor ! JobValidationFailed(jobId, DateTime.now(), err)
          throw err
        case Good(jobData) =>
          // 给前端返回任务已提交状态
          statusActor ! JobStarted(jobId: String, jobInfo)
          val sc = jobContext.sparkContext
          sc.setJobGroup(jobId, s"Job group for $jobId and spark context ${sc.applicationId}", true)
          // 执行任务
          job.runJob(jobC, jobEnv, jobData)
      }
    } finally {
      org.slf4j.MDC.remove("jobId")
    }
  } catch {
    case e: java.lang.AbstractMethodError => {
      logger.error("Oops, there's an AbstractMethodError... maybe you compiled " +
        "your code with an older version of SJS? here's the exception:", e)
      throw e
    }
    case e: Throwable => {
      logger.error("Got Throwable", e)
      throw e
    };
  }
}(executionContext).andThen {
  case Success(result: Any) =>
    // TODO: If the result is Stream[_] and this is running with context-per-jvm=true configuration
    // serializing a Stream[_] blob across process boundaries is not desirable.
    // In that scenario an enhancement is required here to chunk stream results back.
    // Something like ChunkedJobResultStart, ChunkJobResultMessage, and ChunkJobResultEnd messages
    // might be a better way to send results back and then on the other side use chunked encoding
    // transfer to send the chunks back. Alternatively the stream could be persisted here to HDFS
    // and the streamed out of InputStream on the other side.
    // Either way an enhancement would be required here to make Stream[_] responses work
    // with context-per-jvm=true configuration
    // 返回结果给WebUi
    resultActor ! JobResult(jobId, result)
    // 主要更新数据库中任务的状态
    statusActor ! JobFinished(jobId, DateTime.now())
  case Failure(error: Throwable) =>
    // Wrapping the error inside a RuntimeException to handle the case of throwing custom exceptions.
    val wrappedError = wrapInRuntimeException(error)
    // If and only if job validation fails, JobErroredOut message is dropped silently in JobStatusActor.
    statusActor ! JobErroredOut(jobId, DateTime.now(), wrappedError)
    logger.error("Exception from job " + jobId + ": ", error)
}
...
```

5. WebApi.jobRoutes<br/>
还记得第1步中的对任务执行结果的返回处理吗？，在这里，任务提交成功后会返回JobStarted消息，下面是具体的处理方式，记录job的配置并返回结果给前端。
```scala
case JobStarted(_, jobInfo) =>
    val future = jobInfoActor ? StoreJobConfig(jobInfo.jobId, postedJobConfig)
    future.map {
      case JobConfigStored =>
        ctx.complete(202, getJobReport(jobInfo, jobStarted = true))
    }.recover {
      case e: Exception => ctx.complete(500, errMap(e, "ERROR"))
    }
```

至此，一个任务就被成功提交并执行了。

## 总结
看了上面的解读过程，是不是可以总结一下源码阅读的经验呢？下面我来总结一下我的经验。<br/>
1. 在阅读一个项目的源码前，要了解其使用了哪些技术，比如spark-jobserver就用了akka-cluster、spray等，需要做一些知识储备<br/>
2. 先选择少量几个感兴趣的内容阅读，当套路清晰后，再阅读其他内容<br/>
3. 找对阅读的入口，比如上面服务启动中的server_start.sh、接口WebApi中的接口处理逻辑<br/>
4. 主要关心正常分支<br/>
5. 抓住能将整个流程穿起来的线索<br/>
6. 要有耐心<br/>
