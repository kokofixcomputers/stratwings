package certstore

import (
	"encoding/json"
	"net/http"

	"emperror.dev/errors"
	"github.com/apex/log"
)

// maxBody is far more than any certificate with its chain and key.
const maxBody = 256 * 1024

type pushed struct {
	Certificate string `json:"certificate"`
	Key         string `json:"key"`
}

// Handler is the API of the certificate of this node, for the router to mount (behind the authorization of the Panel):
//
//	GET   says which certificate is in use (never the key), so the Panel can show it and knows when to push a new one
//	POST  starts using the certificate and key in the body; a pair that does not work is refused and changes nothing
func Handler(store *Store) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			info, ok := store.Info()
			if !ok {
				reply(w, http.StatusOK, map[string]any{"present": false})
				return
			}
			reply(w, http.StatusOK, map[string]any{"present": true, "certificate": info})
		case http.MethodPost:
			r.Body = http.MaxBytesReader(w, r.Body, maxBody)

			var body pushed
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				reply(w, http.StatusBadRequest, map[string]any{"error": "The request could not be read."})
				return
			}
			if body.Certificate == "" || body.Key == "" {
				reply(w, http.StatusUnprocessableEntity, map[string]any{"error": "Both the certificate and the key are needed."})
				return
			}

			info, err := store.Set([]byte(body.Certificate), []byte(body.Key))
			if err != nil {
				// The reasons are about the certificate that was sent, nothing about this machine.
				reply(w, http.StatusUnprocessableEntity, map[string]any{"error": errors.Cause(err).Error()})
				return
			}

			log.WithField("fingerprint", info.Fingerprint).Info("the Panel pushed a new certificate, it is in use")
			reply(w, http.StatusOK, map[string]any{"present": true, "certificate": info})
		default:
			w.Header().Set("Allow", "GET, POST")
			reply(w, http.StatusMethodNotAllowed, map[string]any{"error": "Method not allowed."})
		}
	})
}

func reply(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
