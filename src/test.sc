val x = Array("Shweta", "Purushe", "Fish")


val y = for (e <- x) yield
  e + "_Ola"


println(y.length)

x contains "Blah";