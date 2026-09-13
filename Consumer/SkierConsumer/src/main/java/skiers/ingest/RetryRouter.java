package skiers.ingest;

import org.springframework.amqp.core.Message;

public interface RetryRouter {

  boolean republish(Message original, int attempt);

  RetryRouter NONE = (original, attempt) -> false;
}
