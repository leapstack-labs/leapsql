// Package main scrapes data-star.dev documentation and saves as markdown files.
//
// The docs page has semantic HTML with all content in one <article> tag.
// Sections are delineated by <h1> headings with IDs matching the sidebar nav.
//
// Usage:
//
//	go run ./scripts/syncdatastardocs
package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	htmltomarkdown "github.com/JohannesKaufmann/html-to-markdown/v2"
	"golang.org/x/net/html"
)

// Configuration.
const (
	docsURL    = "https://data-star.dev/docs"
	contextDir = "context/datastar-docs"
)

// Sections that should be split by H3 headers into separate files.
// These are reference-style docs where each H3 is an independent API item.
var splittableSections = map[string]bool{
	"attributes": true,
	"actions":    true,
}

// Pre-compiled regex patterns.
var (
	reNonWord           = regexp.MustCompile(`[^\w\s-]`)
	reSpacesUnderscores = regexp.MustCompile(`[\s_]+`)
	reMultipleHyphens   = regexp.MustCompile(`-+`)
	reAnchorLinks       = regexp.MustCompile(`\s*\[#\]\(#[\w-]*\)`)
	reLineNumbers       = regexp.MustCompile(`^(\s*)\d{1,4}(.*)$`)
	reExcessiveNewlines = regexp.MustCompile(`\n{4,}`)
	reH3Header          = regexp.MustCompile(`(?m)(^### .+$)`)
	reH3Prefix          = regexp.MustCompile(`^### `)
	reProLink           = regexp.MustCompile(`\[Pro\]\([^)]*\)`)
	reSlugCleanup       = regexp.MustCompile("[`()\\[\\]]")
)

// Section holds content extracted from an H1 section.
type Section struct {
	Title   string
	ID      string
	Content string
}

// Subsection holds content extracted from an H3 subsection.
type Subsection struct {
	Slug    string
	Content string
}

func main() {
	log.Printf("Scraping Data-Star documentation from %s", docsURL)

	// Setup output directory
	if err := setupOutputDir(contextDir); err != nil {
		log.Fatalf("Failed to setup output directory: %v", err)
	}

	// Fetch the docs page
	log.Println("Fetching documentation page...")
	htmlContent, err := fetchPage(docsURL)
	if err != nil {
		log.Fatalf("Failed to fetch page: %v", err)
	}

	// Parse HTML
	doc, err := html.Parse(strings.NewReader(htmlContent))
	if err != nil {
		log.Fatalf("Failed to parse HTML: %v", err)
	}

	// Extract sections by h1 headings
	log.Println("Extracting sections by h1 headings...")
	sections := extractSectionsByH1(doc)
	log.Printf("Found %d sections", len(sections))

	// Save each section as a separate markdown file
	log.Println("Saving sections as markdown files...")
	savedCount := 0

	for _, section := range sections {
		content := cleanMarkdown(section.Content)

		// Skip empty sections
		if content == "" || len(content) < 50 {
			log.Printf("   Skipping empty section: %s", section.Title)
			continue
		}

		// Create filename from ID or title (slugify to convert underscores to hyphens)
		filename := slugify(section.ID)
		if filename == "" {
			filename = slugify(section.Title)
		}
		if filename == "" {
			filename = fmt.Sprintf("section-%d", savedCount)
		}

		// Check if this section should be split by H3 headers
		if splittableSections[filename] {
			log.Printf("   Splitting section '%s' by H3 headers...", section.Title)
			subsections := extractH3Subsections(content)

			if len(subsections) > 1 {
				// Create directory for this section
				sectionDir := filepath.Join(contextDir, filename)
				if err := os.MkdirAll(sectionDir, 0o755); err != nil {
					log.Printf("   Failed to create directory %s: %v", sectionDir, err)
					continue
				}

				for _, sub := range subsections {
					subContent := sub.Content

					// Add main title as H1 for index (if not already present)
					if sub.Slug == "index" {
						if !strings.HasPrefix(subContent, "#") {
							subContent = fmt.Sprintf("# %s\n\n%s", section.Title, subContent)
						}
					} else {
						// Promote H3 to H1 for standalone files
						subContent = reH3Prefix.ReplaceAllString(subContent, "# ")
					}

					subFilepath := filepath.Join(sectionDir, sub.Slug+".md")
					if err := os.WriteFile(subFilepath, []byte(subContent), 0o644); err != nil {
						log.Printf("      Failed to save %s: %v", subFilepath, err)
						continue
					}
					log.Printf("      Saved: %s/%s.md", filename, sub.Slug)
					savedCount++
				}
				continue // Skip normal save
			}
		}

		// Add title as heading if not already present
		if !strings.HasPrefix(content, "#") {
			content = fmt.Sprintf("# %s\n\n%s", section.Title, content)
		}

		fpath := filepath.Join(contextDir, filename+".md")
		if err := os.WriteFile(fpath, []byte(content), 0o644); err != nil {
			log.Printf("   Failed to save %s: %v", fpath, err)
			continue
		}
		log.Printf("   Saved: %s.md", filename)
		savedCount++
	}

	log.Printf("\nSuccess! Scraped %d sections to: %s", savedCount, contextDir)
}

// setupOutputDir removes existing directory and creates a fresh one.
func setupOutputDir(dir string) error {
	if _, err := os.Stat(dir); err == nil {
		log.Printf("Cleaning existing directory: %s", dir)
		if err := os.RemoveAll(dir); err != nil {
			return fmt.Errorf("remove existing directory: %w", err)
		}
	}
	return os.MkdirAll(dir, 0o755)
}

// fetchPage fetches HTML content from a URL.
func fetchPage(pageURL string) (string, error) {
	client := &http.Client{Timeout: 30 * time.Second}

	req, err := http.NewRequest(http.MethodGet, pageURL, nil)
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; DatastarDocsScraper/1.0)")

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("fetch: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read body: %w", err)
	}

	return string(body), nil
}

// slugify converts text to a safe filename slug.
func slugify(text string) string {
	text = strings.ToLower(strings.TrimSpace(text))
	text = reNonWord.ReplaceAllString(text, "")
	text = reSpacesUnderscores.ReplaceAllString(text, "-")
	text = reMultipleHyphens.ReplaceAllString(text, "-")
	return strings.Trim(text, "-")
}

// extractSectionsByH1 extracts sections by splitting on <h1> headings.
func extractSectionsByH1(doc *html.Node) []Section {
	var sections []Section

	// Find the main article tag
	article := findElement(doc, "article")
	if article == nil {
		log.Println("Warning: No main article tag found")
		return sections
	}

	// Find all h1 headings with IDs
	var h1s []*html.Node
	var findH1s func(*html.Node)
	findH1s = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "h1" {
			if id := getAttr(n, "id"); id != "" {
				h1s = append(h1s, n)
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			findH1s(c)
		}
	}
	findH1s(article)

	if len(h1s) == 0 {
		log.Println("Warning: No h1 headings with IDs found")
		return sections
	}

	// For each h1, collect content until next h1
	for i, h1 := range h1s {
		sectionID := getAttr(h1, "id")
		title := strings.TrimRight(getTextContent(h1), "#") // Remove trailing # from title

		// Collect all sibling elements until next h1
		var contentParts []string
		contentParts = append(contentParts, renderNode(h1))

		// Get the next h1 to know when to stop (if any)
		var nextH1 *html.Node
		if i+1 < len(h1s) {
			nextH1 = h1s[i+1]
		}

		// Traverse siblings
		for sibling := h1.NextSibling; sibling != nil; sibling = sibling.NextSibling {
			if sibling == nextH1 {
				break
			}
			if sibling.Type == html.ElementNode && sibling.Data == "h1" {
				break
			}
			contentParts = append(contentParts, renderNode(sibling))
		}

		htmlContent := strings.Join(contentParts, "")

		// Convert to markdown
		mdContent, err := htmltomarkdown.ConvertString(htmlContent)
		if err != nil {
			log.Printf("Warning: failed to convert section %s to markdown: %v", sectionID, err)
			continue
		}

		sections = append(sections, Section{
			Title:   strings.TrimSpace(title),
			ID:      sectionID,
			Content: mdContent,
		})
	}

	return sections
}

// extractH3Subsections splits markdown content by H3 headers into separate subsections.
func extractH3Subsections(content string) []Subsection {
	var subsections []Subsection

	parts := reH3Header.Split(content, -1)
	matches := reH3Header.FindAllString(content, -1)

	// First part is content before any H3 (intro/overview)
	if intro := strings.TrimSpace(parts[0]); intro != "" {
		subsections = append(subsections, Subsection{Slug: "index", Content: intro})
	}

	// Process H3 sections
	for i, match := range matches {
		title := strings.TrimSpace(strings.TrimPrefix(match, "###"))

		var sectionContent string
		if i+1 < len(parts) {
			sectionContent = strings.TrimSpace(parts[i+1])
		}

		fullContent := strings.TrimSpace(match + "\n\n" + sectionContent)

		// Create slug from title (remove backticks, parentheses, [Pro] links, etc.)
		slugText := reProLink.ReplaceAllString(title, "")
		slugText = reSlugCleanup.ReplaceAllString(slugText, "")

		subsections = append(subsections, Subsection{
			Slug:    slugify(slugText),
			Content: fullContent,
		})
	}

	return subsections
}

// cleanMarkdown cleans up markdown content.
func cleanMarkdown(content string) string {
	// Remove anchor links like [#](#section-name) from headings
	content = reAnchorLinks.ReplaceAllString(content, "")

	// Remove line numbers from code blocks
	lines := strings.Split(content, "\n")
	cleanedLines := make([]string, 0, len(lines))
	inCodeBlock := false

	for _, line := range lines {
		if strings.HasPrefix(line, "```") {
			inCodeBlock = !inCodeBlock
			cleanedLines = append(cleanedLines, line)
		} else if inCodeBlock {
			// Remove leading line numbers, preserving everything after
			if match := reLineNumbers.FindStringSubmatch(line); match != nil {
				cleanedLines = append(cleanedLines, match[1]+match[2])
			} else {
				cleanedLines = append(cleanedLines, line)
			}
		} else {
			cleanedLines = append(cleanedLines, line)
		}
	}

	content = strings.Join(cleanedLines, "\n")
	content = reExcessiveNewlines.ReplaceAllString(content, "\n\n\n")

	// Remove trailing whitespace from lines
	lines = strings.Split(content, "\n")
	for i, line := range lines {
		lines[i] = strings.TrimRight(line, " \t")
	}

	return strings.TrimSpace(strings.Join(lines, "\n"))
}

// findElement finds the first element with the given tag name.
func findElement(n *html.Node, tag string) *html.Node {
	if n.Type == html.ElementNode && n.Data == tag {
		return n
	}
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		if found := findElement(c, tag); found != nil {
			return found
		}
	}
	return nil
}

// getAttr returns the value of an attribute, or empty string if not found.
func getAttr(n *html.Node, key string) string {
	for _, attr := range n.Attr {
		if attr.Key == key {
			return attr.Val
		}
	}
	return ""
}

// getTextContent returns the text content of a node and its children.
func getTextContent(n *html.Node) string {
	var sb strings.Builder
	var getText func(*html.Node)
	getText = func(n *html.Node) {
		if n.Type == html.TextNode {
			sb.WriteString(n.Data)
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			getText(c)
		}
	}
	getText(n)
	return strings.TrimSpace(sb.String())
}

// renderNode renders an HTML node back to string.
func renderNode(n *html.Node) string {
	var sb strings.Builder
	if err := html.Render(&sb, n); err != nil {
		return ""
	}
	return sb.String()
}
