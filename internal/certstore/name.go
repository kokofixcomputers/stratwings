package certstore

import "crypto/x509/pkix"

func pkixName(host string) pkix.Name {
	return pkix.Name{CommonName: host, Organization: []string{"Wings (temporary)"}}
}
