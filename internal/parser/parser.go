package parser

import (
	"bytes"
	"strings"

	"golang.org/x/net/html"
)

type Result struct {
	Title string
	Links []string
}

func Parse(body []byte) Result {
	doc, err := html.Parse(bytes.NewReader(body))
	if err != nil {
		return Result{}
	}
	var result Result
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.ElementNode {
			switch strings.ToLower(n.Data) {
			case "title":
				if n.FirstChild != nil && n.FirstChild.Type == html.TextNode {
					result.Title = strings.TrimSpace(n.FirstChild.Data)
				}
			case "a":
				for _, attr := range n.Attr {
					if strings.EqualFold(attr.Key, "href") {
						href := strings.TrimSpace(attr.Val)
						if href != "" && !strings.HasPrefix(href, "#") {
							result.Links = append(result.Links, href)
						}
					}
				}
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(doc)
	return result
}
