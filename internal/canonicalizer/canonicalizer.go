package canonicalizer

import (
	"net/url"
	"sort"
	"strings"
)

var trackingParams = map[string]struct{}{
	"utm_source": {}, "utm_medium": {}, "utm_campaign": {},
	"utm_term": {}, "utm_content": {}, "utm_id": {},
	"fbclid": {}, "gclid": {}, "msclkid": {}, "ref": {}, "source": {},
}

// Normalize returns the canonical form of rawURL resolved against base.
func Normalize(rawURL, base string) (string, error) {
	baseURL, err := url.Parse(base)
	if err != nil {
		return "", err
	}
	ref, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}
	resolved := baseURL.ResolveReference(ref)

	scheme := strings.ToLower(resolved.Scheme)
	if scheme != "http" && scheme != "https" {
		return "", nil
	}

	resolved.Fragment = ""
	resolved.Host = strings.ToLower(resolved.Host)
	resolved.Path = cleanPath(resolved.Path)

	port := resolved.Port()
	if (scheme == "http" && port == "80") || (scheme == "https" && port == "443") {
		resolved.Host = resolved.Hostname()
	}

	if resolved.RawQuery != "" {
		q := resolved.Query()
		for p := range trackingParams {
			delete(q, p)
		}
		if len(q) == 0 {
			resolved.RawQuery = ""
		} else {
			keys := make([]string, 0, len(q))
			for k := range q {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			vals := url.Values{}
			for _, k := range keys {
				vals[k] = q[k]
			}
			resolved.RawQuery = vals.Encode()
		}
	}
	return resolved.String(), nil
}

func cleanPath(p string) string {
	for strings.Contains(p, "//") {
		p = strings.ReplaceAll(p, "//", "/")
	}
	if len(p) > 1 {
		p = strings.TrimRight(p, "/")
	}
	return p
}
