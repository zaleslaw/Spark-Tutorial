// OOP (fields, methods, inheritance)

class Developer(company: String) {
  val skill: String = if (company == "EPAM") {
    "Java"
  } else {
    "Dart"
  }

  def work(hours: Int): Unit = {
    println("I'm working for " + hours + " hours")
  }
}

val javaDeveloper = new Developer("EPAM")
javaDeveloper.work(8)
javaDeveloper.skill

class Manager(company: String) extends Developer(company) {
  val englishLevel: String = "B2"

  def resumeNegotiations() = println(englishLevel)

}

val manager = new Manager("Google")
manager.skill
manager.englishLevel
manager.work(12)
manager.resumeNegotiations()


// traits magic
trait Me {
  def foo: Int
}

trait B extends Me {
  override def foo: Int = 1
}

trait C extends Me {
  override def foo: Int = 2
}

class D extends B with C //можно переставить и будут разные результаты
val d = new D()
d.foo

// Case classes
case class User(name: String,
                email: String)

val vasja = new User("vasja", "1@gmail.com")
val petija = vasja.copy(name = "petija")
petija.name