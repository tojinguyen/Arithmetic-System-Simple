from mini.worker.brokers.rabbitmq import RabbitMQBroker
from mini.worker.result_backends.redis import RedisBackend

RABBITMQ_URI = "amqp://guest:guest@rabbitmq:5672/"
REDIS_URI = "redis://redis:6379/0"

BROKER = RabbitMQBroker(RABBITMQ_URI)
RESULT_BACKEND = RedisBackend(REDIS_URI)