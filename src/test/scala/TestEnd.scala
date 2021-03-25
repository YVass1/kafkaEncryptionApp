import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import com.github.yoginivassaramo.kafka.encryptionapp.ProducerRawMessages.generateRandomWord


class TestEnd extends AnyWordSpec with Matchers with EmbeddedKafka {


  "A Stack" should {
    "pop values in last-in-first-out order" in {
      implicit val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = 9092)

      withRunningKafka {

      }
      1 shouldBe 1
    }
  }

  "generateRandomWord" should {
    "accepts valid file of correct extension" in {
      //variables
      val file: String = "testWordsListConsistent.csv"

      //expected
      val expectedValues: List[String] = List("joined", "palm", "popcorn", "book", "animal", "pen")

      //actual
      val actualValue: String = generateRandomWord(file)

      //comparison
      expectedValues should contain(actualValue)

    }
  }

}