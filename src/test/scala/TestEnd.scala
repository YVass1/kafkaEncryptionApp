import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

object TestEnd extends App {

  class SetSpec extends AnyWordSpec with Matchers with EmbeddedKafka {

    "A Stack" should {
      "pop values in last-in-first-out order" in {
        implicit val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = 9092)

        withRunningKafka {

        }
        1 shouldBe 1
      }
    }

    it should "throw NoSuchElementException if an empty stack is popped" in {
      val emptyStack = new Stack[String]
      assertThrows[NoSuchElementException] {
        emptyStack.pop()
      }
    }




  }
}
