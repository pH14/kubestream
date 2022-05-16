# Kubestream

## About

`kubestream` re-publishes all Kubernetes resource events into a Kafka-compatible downstream. It creates a Watcher for every Kubernetes Resource that supports the `LIST` and `WATCH` verbs, and publishes incoming events into Kafka as JSON.

🚧 In its current form, `kubestream` is not production-grade software 🚧

## Usage

If `kubestream` has access to ambient Kube credentials, simply point it to your favorite Kafka broker:

```
kubestream --kafka-host 127.0.0.1 --kafka-port 9092 --kafka-topic kubestream
```

That's it! Kube events are now streaming into Kafka
