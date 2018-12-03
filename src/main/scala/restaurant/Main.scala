package restaurant

import java.util.Calendar

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Balance, Broadcast, Flow, GraphDSL, Merge, RunnableGraph, Sink, Source}
import akka.stream.{ActorMaterializer, ClosedShape, UniformFanInShape}

import scala.concurrent.duration._
import scala.util.Random
object Main extends App {
  implicit val actorSystem = ActorSystem("Restaurant")
  implicit val materializer = ActorMaterializer()

  // Pizza types
  case class Order(orderType: String, orderNumber: Int) // to provide that will be used at a junction
  // Util
  def throttleTime: Int = {
    var randomTime = Random.nextInt(2)
    if (randomTime == 0) {
      randomTime = 1
    }
     return (randomTime) * 1000  // In milliseconds
  }

  def getTimeStamp(): Int ={
    val calendar = Calendar.getInstance()
    return calendar.getTimeInMillis.toInt
  }


  // Sources - customer orders
  val takeInSource: Source[Order, NotUsed] = {
    Source(1 to 10000)
      .map(_ => Order("TAKE-IN", getTimeStamp()))
        .throttle(1, throttleTime millis)
  }

  val takeOutSource: Source[Order, NotUsed] =
    Source(1 to 10000)
      .map(_ => Order("TAKE-OUT" , getTimeStamp()))
        .throttle(1, throttleTime millis)


  // Batching orders
  val batchedTakeInSource: Flow[Order, Seq[Order], NotUsed] = Flow[Order].grouped(3)
  val batchedTakeOutSource:  Flow[Order, Seq[Order], NotUsed] = Flow[Order].grouped(4)
  // Materialize the orders received so far
  val countFlow = Flow[Seq[Order]].fold(0)((initial, current) => {
    var init = initial
    current.foreach(element => {
      init = init + 1
      print(s"Running count::::::::::::::::::::::${init}")
      print(s":::::::::::::\t${element.orderType} \n")
    })
    init
  })


  // start cooking
  val cookWithOld = Flow[Order].map(a => {
    println(s"Preparing Pizza ${a} ::: OLD OVEN")
    Thread.sleep(5000 )
    a
  })

  val cookWithNew = Flow[Order].map(a => {
    println(s"Preparing Pizza $a ::: ==== NEW OVEN")
    Thread.sleep(2000 )
    a
  })

  val take_out_rack = Flow[Order].map(a => {
    //println(s"Pizza received at take-out-rack :::: PACKAGING")
    a
  })
  val take_in_rack = Flow[Order]map(a => {
    //println(s"Pizza received at take-in-rack :::: PACKAGING")
    a
  })

  val printPizzaAvailableAt = Sink.foreach[Order](a => {
    println(s"Packaged pizza ready at rack ${a.orderType}")})

  val batchAndUpdateRunningCountGraph = GraphDSL.create(){
    implicit b =>
      import GraphDSL.Implicits._
      // Batch customer orders
      val batchTakeIn = b.add(batchedTakeInSource)
      val batchTakeOut = b.add(batchedTakeOutSource)

      // Broadcast batched orders
      val bcastTakeIn = b.add(Broadcast[Seq[Order]](2))
      val bcastTakeOut = b.add(Broadcast[Seq[Order]](2))

      // merge junctions
      val mergeSources = b.add(Merge[Seq[Order]](2))

      // Make the flows
      batchTakeIn ~> bcastTakeIn.in
      batchTakeOut ~> bcastTakeOut.in

      bcastTakeIn.out(0) ~> countFlow.async ~> Sink.ignore
      bcastTakeIn.out(1) ~> mergeSources

      bcastTakeOut.out(0) ~> countFlow.async ~> Sink.ignore
      bcastTakeOut.out(1) ~> mergeSources

      UniformFanInShape(mergeSources.out, batchTakeIn.in, batchTakeOut.in)
  }
  // A partial graph that handles batching, counting and printing order types - takes in two sources - gives out one source

  val prepAndCook = GraphDSL.create() {
    implicit b  =>
      import GraphDSL.Implicits._

      val batchAndCount =  b.add(batchAndUpdateRunningCountGraph)
      val prepCookingBalance = b.add(Balance[Order](3))
      val doneCookingMerge = b.add(Merge[Order](3))

      batchAndCount.out  ~> Flow[Seq[Order]].mapConcat(a => (identity(a.toList)))  ~> prepCookingBalance.in
      prepCookingBalance.out(0) ~> cookWithOld.async ~> doneCookingMerge.in(0)
      prepCookingBalance.out(1) ~> cookWithNew.async ~>  doneCookingMerge.in(1)
      prepCookingBalance.out(2) ~> cookWithNew.async ~>  doneCookingMerge.in(2)

      UniformFanInShape(doneCookingMerge.out, batchAndCount.in(0), batchAndCount.in(1))
  } // Partial graph with two input ports to be connected and one output port

  val finishAndSendToRacksGraph = RunnableGraph.fromGraph(GraphDSL.create(){
    implicit b =>
      import GraphDSL.Implicits._
      val prepCook = b.add(prepAndCook)
      val bcastToFilters = b.add(Broadcast[Order](2))
      takeInSource ~> prepCook.in(0)
      takeOutSource ~> prepCook.in(1)
      prepCook.out ~> bcastToFilters.in
      bcastToFilters.out(0) ~> Flow[Order].filter(a => a.orderType == "TAKE-OUT") ~> take_out_rack.async ~> printPizzaAvailableAt
      bcastToFilters.out(1) ~> Flow[Order].filter(element => element.orderType == "TAKE-IN") ~> take_in_rack.async ~> printPizzaAvailableAt
      ClosedShape
  })

  finishAndSendToRacksGraph.run()










}
