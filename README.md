# RabbitMQ
As RPC Simulator

## Usage 
Start/Stop/Restart rabbitmq service by typing the ff:
```
brew services start rabbitmq
brew services restart rabbitmq
brew services stop rabbitmq
```

Available publishers are: `com.minyodev.rabbitmq.BasicPublish` and `com.minyodev.rabbitmq.MultiplePublish`

Available consumers are: `com.minyodev.rabbitmq.Consumer1` and `com.minyodev.rabbitmq.Consumer2`

Available getters are: `com.minyodev.rabbitmq.Get1` and `com.minyodev.rabbitmq.Get2`