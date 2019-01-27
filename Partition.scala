
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


@SerialVersionUID(123L)
case class Vertices ( cid: Long, vid: Long, adj:List[String])
extends Serializable{}




object Partition {
  val depth = 6
  def reduce_fun(id:Long , s:Iterable[Either[(Long,List[Long]),Long]]): (Long,Long,List[Long]) = 
  {
    var cluster: Long = -1
    for (p <- s)
    {
     
      p match {
        case Right(c) => cluster = c;
        case Left((c,adj)) => {
          if (c>0)
            return (id,c,adj) 
        } 
      }
    }
    return ( id, cluster, adj )
  }
   
  def main ( args: Array[ String ] ) {
      var count: Int =1;
      val conf = new SparkConf().setAppName("Map1").setMaster("local[2]")
      val sc = new SparkContext(conf)
      val inputdata = sc.textFile(args(0)).map( line => {
        var input = line.split(",");
        var vid = input(0).toLong;
        var adj = input.toList.tail;
        var cid: Long =0;
        if(count <= 5)
            cid= vid;
         else
            cid= -1;
        count= count+1;
        Vertices(vid,cid,op)    
        
      })
      for (i <- 1 to depth)
      {
          var count1: Long = -1;
          var flag=0;
              var graph = inputdata.flatMap(d=>{
                                            if(d.cid > count1)
                                            {
                                              val temp1=d.cid;
                                              flag=1;
                                            }
                                            else
                                            {
                                                val temp2=(d.cid,d.op); 
                                                flag=2;
                                            }
                                           }).groupByKey.map{
                                                             if(flag==1)
                                                                reduce_fun(d.vid,temp1)
                                                              else if(flag==2)
                                                                reduce_fun(d.vid,temp2)
                                                            }
          graph.foreach(println);

      }
      
  }
}
