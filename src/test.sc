import scala.Array.concat

//val x = Array("Shweta", "Purushe", "Fish")
//
//
//val y = List(for (e <- x) yield e + "_Ola")
//y.foreach(println)
//y.length
//x contains "Blah";


val columns = Array("Shweta", "Purushe", "Fish","ADOPTION_KEY", "hoopla" ,"COURSE_KEY", "INSTRUCTOR_GUID", "USER_SSO_GUID")
val categorical_features = Array("Color")
val id_columns = Array("ADOPTION_KEY", "COURSE_KEY", "INSTRUCTOR_GUID", "USER_SSO_GUID")
val y = "Species"
val exclude = Array("DISCIPLINE_CATEGORY")

val final_exclude = concat(categorical_features, id_columns, Array(y), exclude)
//
//var s = columns.filterNot(final_exclude contains _);
//println(s)
