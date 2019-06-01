package cn.dmp.utils

object RptUtils {

  def caculateReq (requestmode:Int, processnode:Int) : List[Double] = {
    if(requestmode == 1 && processnode ==1){
      List[Double](1,0,0)
    } else if(requestmode == 1 && processnode ==2){
      List[Double](1,1,0)
    } else if(requestmode == 1 && processnode ==3) {
      List[Double](1,1,1)
    }  else {
      List[Double](0,0,0)
    }
  }

  def caculateRtb(eff:Int, bill:Int, bid:Int, orderId:Int, win:Int, winPrice:Double, adPayMent:Double) : List[Double] = {

    if(eff == 1 && bill == 1 && bid ==1 && orderId != 0){
      List[Double](1,0,0,0)
    } else {
      List[Double](0,0,0,0)
    }
  }

}
