package exchange

import (
	"io"

	"go.uber.org/zap"

	"github.com/gotd/td/crypto"
)

// ClientExchange is a client-side key exchange flow.
type ClientExchange struct {
	unencryptedWriter
	rand io.Reader
	log  *zap.Logger

	keys []PublicKey
	dc   int

	// mode selects permanent vs temporary auth-key generation path.
	mode ExchangeMode
	// expiresIn is only meaningful in temporary mode and forwarded into
	// p_q_inner_data_temp_dc payload.
	expiresIn int
}

// ClientExchangeResult contains client part of key exchange result.
type ClientExchangeResult struct {
	AuthKey    crypto.AuthKey
	SessionID  int64
	ServerSalt int64
	// ServerTime is the server clock (unix seconds) reported in
	// server_DH_inner_data during the exchange. Callers use it to seed the
	// persistent server-time offset so msg_id generation starts aligned with the
	// server even when the local clock is skewed. Zero if unavailable.
	ServerTime int64
	// ExpiresAt is unix timestamp for temporary keys, zero for permanent.
	ExpiresAt int64
}
