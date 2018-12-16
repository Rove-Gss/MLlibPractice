package CodeInBook

import org.apache.spark.{SparkConf, SparkContext}

object SparkMatrixPractice {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("scala Spark Test").setMaster("local[3]")
    val sc = new SparkContext(conf)
    /*
  * part：测试Breeze创建函数
  * description:  Breeze库中有各种线性代数、数值计算的函数。
  */

    //生成全0矩阵
    //    val matrix1 = DenseMatrix.zeros[Double](3,4)
    //    println(matrix1)

    //生成全1矩阵
    //    val matrix = DenseMatrix.ones[Double](5,6)
    ////    println(matrix)

    //生成单值矩阵
    //    val matrix = DenseMatrix.fill(3,4){3.0}
    //    println(matrix)

    //生成顺序向量
    //    val vector = DenseVector.range(1,10,2)
    //    println(vector)
    //生成单位矩阵
    //    val matrix = DenseMatrix.eye[Double](3)
    //    println(matrix)

    //生成矩阵
    //    val matrix = DenseMatrix((1.0,2.0),(3.0,4.0))
    //    println(matrix)
    //测试转置
    //    val matrix = DenseMatrix((1.0,2.0),(3.0,4.0)).t
    //    println(matrix)

    //从函数创建向量
    //    val vector = DenseVector.tabulate(5){i => i*i}
    //    println(vector)//从0开始

    //从函数创建矩阵
    //    val matrix = DenseMatrix.tabulate(3,3){case (i,j)=> i+j}
    //    println(matrix)//i,j同样从0开始

    //随机建立向量
    //    val Vector = DenseVector.rand(4)
    //    println(Vector)//随机值在0-1之间

    /*
  * part：测试Breeze访问元素以及操作函数
  * description:
  */

    //测试向量元素访问
    //    val v1 = new DenseVector(Array(1,2,3,4,5,6,7,8))
    //    println(v1(1))//2
    //    println(v1(1 to 4))//DenseVector(2, 3, 4, 5)
    //    println(v1(5 to 0 by -1))//DenseVector(6, 5, 4, 3, 2, 1)
    //    println(v1(1 to -1))//DenseVector(2, 3, 4, 5, 6, 7, 8)
    //    println(v1(-1))//8

    //测试矩阵重塑
    //    val matrix = DenseMatrix((1.0,2.0),(4.0,5.0),(7.0,8.0))
    //    println(matrix.reshape(2,3))

    //测试矩阵转向量
    //    val matrix = DenseMatrix((1.0,2.0,3.0),(4.0,5.0,6.0))
    //    println(matrix.toDenseVector)

    //测试复制矩阵上下三角
    //    val matrix = DenseMatrix((1.0,2.0,3.0),(4.0,5.0,6.0),(7.0,8.0,9.0))
    //    println(upperTriangular(matrix))
    //    println(lowerTriangular(matrix))

    //测试矩阵复制
    //    val matrix = DenseMatrix((1.0,2.0,3.0),(4.0,5.0,6.0),(7.0,8.0,9.0))
    //    println(matrix.copy)

    //测试获取矩阵对角元素
    //    val matrix = DenseMatrix((1.0,2.0,3.0),(4.0,5.0,6.0),(7.0,8.0,9.0))
    //    println(diag(matrix))

    //测试子集赋数值
    //    val matrix = DenseMatrix((1.0,2.0,3.0),(4.0,5.0,6.0),(7.0,8.0,9.0))
    //
    //    matrix(::,1):=5.0//从0开始
    //    println(matrix)

    //测试向量赋值
    //    val vector = new DenseVector(Array(1.0,2.0,3.0,4.0,5.0))
    //    vector(0) = 9.0
    //    println(vector)
    //    vector(1 to 4):= 9.0
    //    println(vector)

    //测试矩阵连接
    //    val matrixA = DenseMatrix((1.0,2.0,3.0))
    //    val matrixB = DenseMatrix((4.0,5.0,6.0))
    //    println(DenseMatrix.vertcat(matrixA,matrixB))
    //    println(DenseMatrix.horzcat(matrixA,matrixB))

    /*
  * part：测试Breeze数值计算函数
  * description:
  */

    //测试元素加减乘除
    //    val matrixA = DenseMatrix((1.0,2.0,3.0),(4.0,5.0,6.0))
    //    val matrixB = DenseMatrix((1.0,2.0,3.0),(4.0,5.0,6.0))
    //    println(matrixA + matrixB)
    //    println(matrixA - matrixB)
    //    println(matrixA :* matrixB)//注意不是直接乘，对应元素相乘
    //    println(matrixA :/ matrixB)//同样，不是直接除

    //测试元素大于、小于、等于
    //    val matrixA = DenseMatrix((1.0,2.0,3.0),(4.0,5.0,6.0))
    //    val matrixB = DenseMatrix((1.0,1.0,4.0),(8.0,2.0,12.0))
    //    println(matrixA :> matrixB)
    //    println(matrixA :< matrixB)
    //    println(matrixA :== matrixB)//注意是两个等号

    //测试元素自加、乘
    //    val matrixA = DenseMatrix((1.0, 2.0, 3.0), (4.0, 5.0, 6.0))
    //    println(matrixA :+= 1.0)
    //    println(matrixA :*= 2.0)

    //测试向量点积
    //    val Vector1 = new DenseVector(Array(1.0,2.0,3.0,4.0))
    //    val Vector2 = new DenseVector(Array(1.0,2.0,3.0,4.0))
    //    println(Vector1 dot Vector2)

    /*
  * part：测试Breeze求和函数
  * description:
  */

    //测试元素求和
    //    val matrixA = DenseMatrix((1.0,2.0,3.0),(4.0,5.0,6.0),(7.0,8.0,9.0))
    //    val Vector1 = new DenseVector(Array(1.0,2.0,3.0,4.0))
    //    println(sum(matrixA))
    //    println(sum(matrixA,Axis._0))
    //    println(sum(matrixA,Axis._1))
    //    println(trace(matrixA))
    //    println(accumulate(Vector1))

    /*
 * part：测试Breeze布尔函数
 * description:
 */

    //测试元素与或非操作
    //    val Vector1 = new DenseVector(Array(true,true,false,false))
    //    val Vector2 = new DenseVector(Array(true,false,true,false))
    //    println(Vector1:&Vector2)
    //    println(Vector1:|Vector2)
    //    println(!Vector1)

    //测试任意元素非0、所有元素非0
    //    val Vector1 = new DenseVector(Array(true,true,false,false))
    //    println(any(Vector1))
    //    println(all(Vector1))

    /*
 * part：测试Breeze线性代数函数
 * description:
 */

    //测试矩阵除法，貌似并不是矩阵除法
    //    val matrixA = DenseMatrix((1.0,4.0,3.0),(4.0,5.0,6.0),(7.0,8.0,8.0))
    //    val matrixB = DenseMatrix((1.0,2.0,3.0),(4.0,5.0,6.0),(7.0,8.0,9.0))
    //    println(matrixA / matrixB)

    //测试矩阵转置
    //    val matrixA = DenseMatrix((1.0,2.0,3.0),(4.0,5.0,6.0),(7.0,8.0,9.0))
    //    println(matrixA.t)

    //测试矩阵求特征值
    //    val matrixA = DenseMatrix((1.0,0.0,0.0),(0.0,5.0,0.0),(0.0,0.0,9.0))
    //    println(det(matrixA))

    //测试求矩阵的逆
    //    val matrixA = DenseMatrix((1.0,0.0,0.0),(0.0,5.0,0.0),(0.0,0.0,9.0))
    //    println(inv(matrixA))

    //测试求矩阵的伪逆，伪逆X满足条件：AXA=A,XAX=X
    //    val matrixA = DenseMatrix((1.0,0.0,0.0),(0.0,5.0,0.0),(0.0,0.0,0.0))
    //    println(pinv(matrixA))

    //测试求矩阵范数
    //    val matrixA = DenseMatrix((1.0,0.0,0.0),(0.0,5.0,0.0),(0.0,0.0,0.0))
    //    println(norm(matrixA))

    //测试获取矩阵特征值与特征向量
    //    val matrixA = DenseMatrix((1.0,0.0,0.0),(0.0,5.0,0.0),(0.0,0.0,9.0))
    //    println(eigSym(matrixA))

    //测试获取矩阵特征向量
    //    val matrixA = DenseMatrix((1.0,0.0,0.0),(0.0,5.0,0.0),(0.0,0.0,9.0))
    //    println(eig(matrixA))

    //测试获取矩阵的秩
    //    val matrixA = DenseMatrix((1.0,0.0,0.0),(0.0,5.0,0.0),(0.0,0.0,0.0))
    //    println(rank(matrixA))

    /*
* part：测试Breeze取整函数
* description:
*/

    //测试向量四舍五入
    //    val Vector1 = new DenseVector(Array(1.2,2.4,3.5,4.9))
    //    println(round(Vector1))

    //测试获取向量最小整数、最大整数、符号函数、取正数
    //    val Vector1 = new DenseVector(Array(1.2,2.4,3.5,4.9))
    //    println(ceil(Vector1))//向上取整
    //    println(floor(Vector1))//向下取整
    //    println(signum(Vector1))
    //    println(abs(Vector1))

    /*
* part：测试Breeze常量函数
* description: 非数：NaN 无穷大：inf pi:Constants.Pi 以e为底的指数:Constants.E
*/


    /*
* part：测试Breeze复数函数
* description: 虚数单位：i
*/


    /*
* part：测试BLAS
* description: BLAS是基础线性代数程序集
*/


    /*
* part：测试MLlib分布式矩阵
* description: 分布式矩阵的数据分块或者分行储存，实现矩阵的基本运算。
*/

    //测试行矩阵
    //    val VectorArrayRDD = sc.parallelize(Array(Array(1.0,2.0,3.0,4.0),Array(2.0,3.0,4.0,5.0),Array(3.0,4.0,5.0,6.0))).map(f =>Vectors.dense(f))
    //    val RM = new RowMatrix(VectorArrayRDD)
    //    println(RM.rows.collect().foreach(println))

    //计算行矩阵每列之间的相似度 为啥是每列？难道不该是每行？
    //    val VectorArrayRDD = sc.parallelize(Array(Array(1.0, 2.0, 3.0, 4.0), Array(2.0, 3.0, 4.0, 5.0), Array(3.0, 4.0, 5.0, 6.0))).map(f => Vectors.dense(f))
    //    val RM = new RowMatrix(VectorArrayRDD)
    //
    //    val simic2 = RM.columnSimilarities(0.5)
    //    simic2.entries.collect().foreach(println)

    //计算每列的汇总统计
    //    val VectorArrayRDD = sc.parallelize(Array(Array(1.0, 2.0, 3.0, 4.0), Array(2.0, 3.0, 4.0, 5.0), Array(3.0, 4.0, 5.0, 6.0))).map(f => Vectors.dense(f))
    //    val RM = new RowMatrix(VectorArrayRDD)
    //    val simic2 = RM.computeColumnSummaryStatistics()
    //    println(simic2.max)
    //    println(simic2.min)
    //    println(simic2.mean)

    //计算每列之间的协方差，生成协方差矩阵
    //    val VectorArrayRDD = sc.parallelize(Array(Array(1.0, 2.0, 3.0, 4.0), Array(2.0, 3.0, 4.0, 5.0), Array(3.0, 4.0, 5.0, 6.0))).map(f => Vectors.dense(f))
    //    val RM = new RowMatrix(VectorArrayRDD)
    //    val cc1 = RM.computeCovariance()
    //    println(cc1.toString())

    //计算格拉姆矩阵
    //        val VectorArrayRDD = sc.parallelize(Array(Array(1.0, 2.0, 3.0, 4.0), Array(2.0, 3.0, 4.0, 5.0), Array(3.0, 4.0, 5.0, 6.0))).map(f => Vectors.dense(f))
    //        val RM = new RowMatrix(VectorArrayRDD)
    //        val cc1 = RM.computeGramianMatrix()
    //        println(cc1.toString())

    //测试主成分分析
    //    val VectorArrayRDD = sc.parallelize(Array(Array(1.0, 2.0, 3.0, 4.0), Array(2.0, 3.0, 4.0, 5.0), Array(3.0, 4.0, 5.0, 6.0))).map(f => Vectors.dense(f))
    //    val RM = new RowMatrix(VectorArrayRDD)
    //    val pc1 = RM.computePrincipalComponents(3)
    //    println(pc1.toString())

    //矩阵奇异值分解
    //    val VectorArrayRDD = sc.parallelize(Array(Array(1.0, 2.0, 3.0, 4.0), Array(2.0, 3.0, 4.0, 5.0), Array(3.0, 4.0, 5.0, 6.0))).map(f => Vectors.dense(f))
    //    val RM = new RowMatrix(VectorArrayRDD)
    //    val svd = RM.computeSVD(4,true)
    //    val U = svd.U
    //    U.rows.foreach(println)

    //测试行索引矩阵

    //测试坐标矩阵
  }
}
