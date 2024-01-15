import org.apache.spark.sql.DataFrame

import scala.Console.println
import scala.meta._
import scala.reflect.runtime.universe
import scala.util.Try

trait TagExtractorTrait {
  def callFunctionByName(className: String, functionName: String, params: Seq[Any]): Any
  def processChecks(df: DataFrame, checks: Array[String]): List[(String, Any)]
  def tree_traverser(tree:Source,qualityTags:IndexedSeq[String]):List[(String, Any)]
}
object TagExtractor extends TagExtractorTrait{

  case class FileContent(comments: IndexedSeq[String], parsed: Parsed[Source])

  def callFunctionByName(className: String, functionName: String, params: Seq[Any]): Any = {
    val mirror = universe.runtimeMirror(getClass.getClassLoader)
    val module = mirror.staticModule(className)
    val im = mirror.reflectModule(module)
    val methodSymbol = im.symbol.typeSignature.member(universe.TermName(functionName)).asMethod
    val method = mirror.reflect(im.instance).reflectMethod(methodSymbol)
    try {
      method(params: _*)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        throw e
    }
  }

  def extractComments_and_parsedCode_FromFile(filePath: String): FileContent = {
    val scalaCode = scala.io.Source.fromFile(filePath).mkString
    val tokens = scalaCode.tokenize.get
    val comments = tokens.collect {
      case Token.Comment(comment) => comment
    }
    val parsed: Parsed[Source] = scalaCode.parse[Source]
    FileContent(comments, parsed)
  }

  @throws(classOf[Exception])
  def processChecks(df: DataFrame, checks: Array[String]): List[(String, Any)] = {
    var results = List.empty[(String, Any)]
    checks.foreach { check =>
      val parts = check.split("[(),]")
      val className = parts(0).trim.capitalize // Assuming the classname matches the function name but is capitalized
      println(s"ClassName: $className")
      val functionName = parts(0).trim
      println(s"FunctionName: $functionName")
      val parameters = Seq(df) ++ parts.drop(1).map(_.replace("[", "").replace("]", "").trim)
      //println(s"Parameters: ${parameters.mkString(", ")}")
      val result = Try {
        val result = callFunctionByName(s"$className", functionName, parameters)
      }

      result match {
        case scala.util.Success(value) =>
          results = results :+ (s"$className.$functionName", value)
        case scala.util.Failure(exception) =>
          // Handle the exception or log an error
          println(s"Error processing check: $check - ${exception.getMessage}")
          // You can also include an error indicator in the results
          results = results :+ (s"$className.$functionName", s"Error: ${exception.getMessage}")
      }
    }
    results
  }

  def tree_traverser(tree:Source,qualityTags:IndexedSeq[String]):List[(String, Any)] = {
    var functionToQuality = Map.empty[String, String]
    var results  = List.empty[(String, Any)]
    tree.traverse {
      case q"..$mods  def $name[..$tparams](...$paramss): $tpeopt = $expr" =>
        val funcName = name.value
        val funcParams = paramss.flatten.map(_.name.value).mkString(", ")
        // Look for the corresponding quality tag
        val associatedQualityTag = qualityTags.find(_.contains(s"@quality"))
        associatedQualityTag.foreach(tag => {
          val tagContent = tag.substring(tag.indexOf("@quality") + 8)
          functionToQuality += (funcName -> tagContent)
          println(s"Function Name: $funcName")
          println(s"@quality content: $tagContent\n")
          println(s"Function Parameters: $funcParams")
          println(s"Function body: ${expr.syntax}\n")
          println(s"Function output: ${tpeopt}\n")
          val splitTag = tagContent.replace("@quality", "").split(";")
          val dfGenClassName = "RandomQuality" // Replace with your logic to determine the class name
          val dfGenFuncName = funcName // Use the function name as the method to be called
          val df: DataFrame = callFunctionByName(dfGenClassName, funcName, Seq.empty).asInstanceOf[DataFrame]
          //println(df.show())
          results = processChecks(df, splitTag.map(_.trim))
        })
      case _ =>
    }
    results
  }

  def main(args: Array[String]): Unit = {
    val filePath = "/Users/khalid/IdeaProjects/untitled/src/main/scala/RandomQuality.scala"
    val result = extractComments_and_parsedCode_FromFile(filePath)
    val comments = result.comments
    val parsed =result.parsed
    val qualityTags = comments.filter(_.contains("@quality"))
    //val parsed: Parsed[Source] = scalaCode.parse[Source]
    parsed match {
      case Parsed.Success(tree) =>
        val results = tree_traverser(tree, qualityTags)
        println(results)
      case Parsed.Error(pos, message, _) =>
        println(s"Error parsing source code: $message")
    }
  }

}
