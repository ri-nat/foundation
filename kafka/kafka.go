package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

const (
	HeaderCorrelationID = "correlation-id"
	HeaderOriginatorID  = "originator-id"
	HeaderProtoName     = "proto-name"
)

const (
	ConsumerComponentName = "kafka-consumer"
	ProducerComponentName = "kafka-producer"
)

type ConsumerComponent struct {
	Consumer *kafka.Reader

	appName       string
	brokers       []string
	logger        *logrus.Entry
	saslMechanism sasl.Mechanism
	topics        []string
	tlsDir        string
}

// ConsumerComponentOption represents an option for the ConsumerComponent
type ConsumerComponentOption func(*ConsumerComponent)

// WithConsumerAppName sets the app name for the ConsumerComponent
func WithConsumerAppName(appName string) ConsumerComponentOption {
	return func(c *ConsumerComponent) {
		c.appName = appName
	}
}

// WithConsumerBrokers sets the brokers for the ConsumerComponent
func WithConsumerBrokers(brokers []string) ConsumerComponentOption {
	return func(c *ConsumerComponent) {
		c.brokers = brokers
	}
}

// WithConsumerLogger sets the logger for the ConsumerComponent
func WithConsumerLogger(logger *logrus.Entry) ConsumerComponentOption {
	return func(c *ConsumerComponent) {
		c.logger = logger.WithField("component", ConsumerComponentName)
	}
}

// WithConsumerTopics sets the topics for the ConsumerComponent
func WithConsumerTopics(topics []string) ConsumerComponentOption {
	return func(c *ConsumerComponent) {
		c.topics = topics
	}
}

// WithConsumerTLSDir sets the location of the TLS directory for the ConsumerComponent
func WithConsumerTLSDir(tlsDir string) ConsumerComponentOption {
	return func(c *ConsumerComponent) {
		c.tlsDir = tlsDir
	}
}

// WithSASLMechanism sets the sasl mechanism for the ConsumerComponent
func WithSASLMechanism(protocol, username, password string) (ConsumerComponentOption, error) {
	mechanism, err := newSASLMechanism(protocol, username, password)
	if err != nil {
		return nil, err
	}

	return func(c *ConsumerComponent) {
		c.saslMechanism = mechanism
	}, err
}

// NewConsumerComponent returns a new ConsumerComponent
func NewConsumerComponent(opts ...ConsumerComponentOption) *ConsumerComponent {
	c := &ConsumerComponent{}

	for i := range opts {
		opts[i](c)
	}

	return c
}

// Start implements the Component interface.
func (c *ConsumerComponent) Start() error {
	if len(c.topics) == 0 {
		return errors.New("you must specify topics during the application initialization using the `WithKafkaConsumerTopics`")
	}
	c.logger.Debugf("Kafka consumer topics: %v", c.topics)

	config := kafka.ReaderConfig{
		Brokers:     c.brokers,
		GroupID:     fmt.Sprintf("%s-foundation", c.appName),
		GroupTopics: c.topics,
		ErrorLogger: c.logger,
	}

	dialer, err := newDialer(c.tlsDir, c.saslMechanism)
	if err != nil {
		return err
	}

	config.Dialer = dialer

	consumer := kafka.NewReader(config)

	c.Consumer = consumer

	return nil
}

// Stop implements the Component interface.
func (c *ConsumerComponent) Stop() error {
	return c.Consumer.Close()
}

// Health implements the Component interface.
func (c *ConsumerComponent) Health() error {
	if c.Consumer == nil {
		return errors.New("reader is not initialized")
	}

	// TODO: find a way to check the health of the consumer.

	return nil
}

// Name implements the Component interface.
func (c *ConsumerComponent) Name() string {
	return ConsumerComponentName
}

// ProducerComponent represents a Kafka producer component
type ProducerComponent struct {
	Producer *kafka.Writer

	brokers       []string
	logger        *logrus.Entry
	tlsDir        string
	batchSize     int
	batchTimeout  time.Duration
	saslMechanism sasl.Mechanism
}

// ProducerComponentOption represents an option for the ProducerComponent
type ProducerComponentOption func(*ProducerComponent)

// WithProducerSASLMechanism sets the sasl mechanism for the ProducerComponent
func WithProducerSASLMechanism(protocol, username, password string) (ProducerComponentOption, error) {
	mechanism, err := newSASLMechanism(protocol, username, password)
	if err != nil {
		return nil, err
	}

	return func(c *ProducerComponent) {
		c.saslMechanism = mechanism
	}, err
}

// WithProducerBrokers sets the brokers for the ProducerComponent
func WithProducerBrokers(brokers []string) ProducerComponentOption {
	return func(c *ProducerComponent) {
		c.brokers = brokers
	}
}

// WithProducerLogger sets the logger for the ProducerComponent
func WithProducerLogger(logger *logrus.Entry) ProducerComponentOption {
	return func(c *ProducerComponent) {
		c.logger = logger.WithField("component", ProducerComponentName)
	}
}

// WithProducerTLSDir sets the location of the TLS files for the ProducerComponent
func WithProducerTLSDir(tlsDir string) ProducerComponentOption {
	return func(c *ProducerComponent) {
		c.tlsDir = tlsDir
	}
}

// WithProducerBatchSize sets the batching size for the ProducerComponent
func WithProducerBatchSize(batchSize int) ProducerComponentOption {
	return func(c *ProducerComponent) {
		c.batchSize = batchSize
	}
}

// WithProducerBatchTimeout sets the batching timeout for the ProducerComponent
func WithProducerBatchTimeout(batchTimeout time.Duration) ProducerComponentOption {
	return func(c *ProducerComponent) {
		c.batchTimeout = batchTimeout
	}
}

// NewProducerComponent returns a new ProducerComponent
func NewProducerComponent(opts ...ProducerComponentOption) *ProducerComponent {
	c := &ProducerComponent{}

	for i := range opts {
		opts[i](c)
	}

	return c
}

// Start implements the Component interface.
func (c *ProducerComponent) Start() error {
	transport, err := newTransport(c.tlsDir, c.saslMechanism)
	if err != nil {
		return err
	}

	producer := &kafka.Writer{
		Addr:                   kafka.TCP(c.brokers...),
		AllowAutoTopicCreation: true,
		BatchSize:              c.batchSize,
		BatchTimeout:           c.batchTimeout,
		Logger:                 c.logger,
		Transport:              transport,
		Balancer:               &kafka.Hash{}, // distribute messages to partitions based on the hash of the key, round-robin if no key
	}

	c.Producer = producer

	return nil
}

// Stop implements the Component interface.
func (c *ProducerComponent) Stop() error {
	return c.Producer.Close()
}

// Health implements the Component interface.
func (c *ProducerComponent) Health() error {
	if c.Producer == nil {
		return errors.New("writer is not initialized")
	}

	return nil
}

// Name implements the Component interface.
func (c *ProducerComponent) Name() string {
	return ProducerComponentName
}

func newDialer(tlsDir string, saslMechanism sasl.Mechanism) (*kafka.Dialer, error) {
	if tlsDir == "" && saslMechanism == nil {
		return nil, nil
	}

	var (
		tlsConfig *tls.Config
		err       error
	)

	if tlsDir != "" {
		tlsConfig, err = newTLSConfig(tlsDir)
	}

	if err != nil {
		return nil, err
	}

	dialer := &kafka.Dialer{
		Timeout:       10 * time.Second,
		DualStack:     true,
		TLS:           tlsConfig,
		SASLMechanism: saslMechanism,
	}

	return dialer, nil
}

func newTransport(tlsDir string, saslMechanism sasl.Mechanism) (*kafka.Transport, error) {
	if tlsDir == "" && saslMechanism == nil {
		return &kafka.Transport{}, nil
	}

	var (
		tlsConfig *tls.Config
		err       error
	)

	if tlsDir != "" {
		tlsConfig, err = newTLSConfig(tlsDir)
	}

	if err != nil {
		return nil, err
	}

	return &kafka.Transport{
		TLS:  tlsConfig,
		SASL: saslMechanism,
	}, nil
}

func newTLSConfig(dir string) (*tls.Config, error) {
	certFile := filepath.Join(dir, "tls.crt")
	keyFile := filepath.Join(dir, "tls.key")
	caFile := filepath.Join(dir, "ca.crt")

	tlsConfig := &tls.Config{}

	_, certFileErr := os.Stat(certFile)
	_, keyFileErr := os.Stat(keyFile)
	_, caFileErr := os.Stat(caFile)

	if certFileErr != nil && keyFileErr != nil && caFileErr != nil {
		return nil, errors.Join(certFileErr, keyFileErr, caFileErr)
	}

	// Load client cert
	if certFileErr == nil && keyFileErr == nil {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, err
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	// Load CA cert
	if caFileErr == nil {
		caCert, err := os.ReadFile(caFile)
		if err != nil {
			return nil, err
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		tlsConfig.RootCAs = caCertPool
	}

	return tlsConfig, nil
}

// newSASLMechanism return a SASL mechanism
// Available values for protocol are "plain" or "scram-sha-512"
func newSASLMechanism(protocol, username, password string) (sasl.Mechanism, error) {
	var (
		mechanism sasl.Mechanism
		err       error
	)

	switch strings.ToLower(protocol) {
	case "scram-sha-512":
		mechanism, err = scram.Mechanism(scram.SHA512, username, password)
	case "plain":
		mechanism = plain.Mechanism{
			Username: username,
			Password: password,
		}
	default:
		err = fmt.Errorf("unknown protocol %s. available values for protocol are \"plain\" or \"scram-sha-512\"", protocol)
	}

	return mechanism, err
}
