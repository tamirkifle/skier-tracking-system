package skiers.ingest;

public interface DeliveryAck {

  void ack();

  void reject(boolean requeue);

  boolean retryLater(int attempt);

  DeliveryAck NONE =
      new DeliveryAck() {
        @Override
        public void ack() {}

        @Override
        public void reject(boolean requeue) {}

        @Override
        public boolean retryLater(int attempt) {
          return false;
        }
      };
}
