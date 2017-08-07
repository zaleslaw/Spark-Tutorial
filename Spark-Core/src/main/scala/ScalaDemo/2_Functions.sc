// Functions and objects
// functions

def addTen(a: Int): Int = a + 10

def multipleTen(a: Int): Int = a * 10

val result = addTen(1) + multipleTen(1)

// function without def
val anonFunction = (x: Int) => x + 10
val newResult = anonFunction(1) + multipleTen(1)


//isPrime function
def isPrime(n: Int) = n != 1 && (2 until n).forall(n % _ != 0)
for {
  i <- 1 to 1000
  if isPrime(i)
} print(i + " ")

//Do you like factorials?
def fact(n: Int): BigInt = {
  if (n <= 1) 1
  else fact(n - 1) * n
}

fact(100)