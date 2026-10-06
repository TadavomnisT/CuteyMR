// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (c) 2025 The CuteyMR Authors

// Command mr-mockgen generates deterministic mock text documents
// (themed "books") used to demo and test the CuteyMR wordcount job.
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
)

var commonWords = []string{
	"the", "of", "and", "to", "a", "in", "that", "is", "was", "he", "for", "it",
	"with", "as", "his", "on", "be", "at", "by", "i", "this", "had", "not",
	"are", "but", "from", "or", "have", "an", "they", "which", "one", "you",
	"were", "her", "all", "she", "there", "would", "their", "we", "him", "been",
	"has", "when", "who", "will", "more", "no", "if", "out", "so", "said",
	"what", "up", "its", "about", "into", "than", "them", "can", "only",
	"other", "new", "some", "could", "time", "these", "two", "may", "then",
	"do", "first", "any", "my", "now", "such", "like", "our", "over", "man",
	"me", "even", "most", "made", "after", "also", "did", "many", "before",
	"must", "through", "back", "years", "where", "much", "your", "way", "well",
	"down", "should", "because", "each", "just", "those", "people", "mr",
	"how", "too", "little", "state", "good", "very", "make", "world", "still",
	"own", "see", "men", "work", "long", "get", "here", "between", "both",
	"life", "being", "under", "never", "day", "same", "another", "know",
	"while", "last", "might", "us", "great", "old", "year", "off", "come",
	"since", "against", "go", "came", "right", "used", "take", "three",
	"story", "night", "hand", "small", "house", "place", "point", "part",
}

var themes = map[string][]string{
	"space": {"star", "stars", "galaxy", "nebula", "orbit", "rocket", "planet",
		"comet", "asteroid", "lunar", "solar", "cosmos", "void", "quasar",
		"pulsar", "horizon", "gravity", "saturn", "venus", "jupiter",
		"telescope", "eclipse", "meteor", "dark", "light", "engine", "ship",
		"captain", "voyage", "signal", "beacon", "hull", "thrusters"},
	"ocean": {"sea", "wave", "waves", "tide", "coral", "reef", "fish", "whale",
		"shark", "sailor", "harbor", "island", "salt", "deep", "current",
		"storm", "sail", "anchor", "lighthouse", "gull", "shell", "pearl",
		"diver", "kelp", "tidepool", "moon", "tide", "foghorn", "deckhand"},
	"forest": {"tree", "trees", "root", "roots", "river", "moss", "fern",
		"wolf", "deer", "owl", "pine", "cedar", "trail", "leaf", "leaves",
		"branch", "bark", "rain", "mist", "stone", "brook", "mushroom",
		"squirrel", "canopy", "shadow", "dawn", "thicket", "acorn"},
	"kitchen": {"bread", "oven", "flour", "salt", "butter", "pan", "pot",
		"spoon", "chef", "recipe", "simmer", "broth", "onion", "garlic",
		"herb", "knife", "plate", "table", "window", "morning", "coffee",
		"cup", "cake", "sugar", "honey", "cream", "kettle", "apron"},
	"music": {"note", "notes", "song", "melody", "chord", "string", "piano",
		"violin", "drum", "rhythm", "tempo", "silence", "concert", "stage",
		"singer", "chorus", "echo", "tune", "harmony", "measure", "score",
		"bow", "key", "minor", "major", "scale", "encore", "ballad"},
}

type book struct {
	name   string
	theme  string
	target int64 // bytes
}

func main() {
	var (
		out   = flag.String("out", "data/input", "output directory")
		seed  = flag.Int64("seed", 20240915, "deterministic RNG seed")
		scale = flag.Int("scale", 1, "multiply the size of every generated book")
	)
	flag.Parse()

	if err := os.MkdirAll(*out, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}
	books := []book{
		{"voyage_of_the_star_whale.txt", "space", 900_000 * int64(*scale)},
		{"songs_of_the_deep.txt", "ocean", 700_000 * int64(*scale)},
		{"roots_and_rivers.txt", "forest", 600_000 * int64(*scale)},
		{"the_glass_kitchen.txt", "kitchen", 500_000 * int64(*scale)},
		{"notes_from_the_attic.txt", "music", 350_000 * int64(*scale)},
		{"field_notes_quick.txt", "mixed", 30_000 * int64(*scale)},
		{"punctuated_letters.txt", "kitchen", 25_000 * int64(*scale)},
	}
	rng := rand.New(rand.NewSource(*seed))

	// zipf-ish weights over common words (rank 0 most frequent)
	weights := make([]float64, len(commonWords))
	total := 0.0
	for i := range commonWords {
		w := 1.0 / float64(i+1)
		weights[i] = w
		total += w
	}
	cum := make([]float64, len(commonWords))
	acc := 0.0
	for i, w := range weights {
		acc += w
		cum[i] = acc / total
	}
	pickCommon := func() string {
		x := rng.Float64()
		lo, hi := 0, len(cum)-1
		for lo < hi {
			mid := (lo + hi) / 2
			if cum[mid] < x {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		return commonWords[lo]
	}
	pickTheme := func(theme string) string {
		if theme == "mixed" {
			keys := make([]string, 0, len(themes))
			for k := range themes {
				keys = append(keys, k)
			}
			theme = keys[rng.Intn(len(keys))]
		}
		words := themes[theme]
		return words[rng.Intn(len(words))]
	}

	var totalBytes int64
	for _, b := range books {
		var sb strings.Builder
		for sb.Len() < int(b.target) {
			n := 6 + rng.Intn(13)
			words := make([]string, 0, n)
			for i := 0; i < n; i++ {
				if rng.Float64() < 0.72 {
					words = append(words, pickCommon())
				} else {
					words = append(words, pickTheme(b.theme))
				}
			}
			sentence := strings.Join(words, " ")
			sentence = strings.ToUpper(sentence[:1]) + sentence[1:]
			switch rng.Intn(10) {
			case 0:
				sentence += "!"
			case 1:
				sentence += "?"
			case 2:
				sentence += ";"
			case 3:
				sentence += ","
			default:
				sentence += "."
			}
			if rng.Intn(6) == 0 && n > 8 {
				mid := n / 2
				parts := strings.SplitN(sentence, " ", -1)
				parts = append(parts[:mid], append([]string{","}, parts[mid:]...)...)
				sentence = strings.Join(parts, " ")
			}
			if rng.Intn(12) == 0 {
				sentence = "\"" + sentence + "\""
			}
			sb.WriteString(sentence)
			sb.WriteString(" ")
			if rng.Intn(7) == 0 {
				sb.WriteString("\n")
			}
		}
		path := filepath.Join(*out, b.name)
		if err := os.WriteFile(path, []byte(sb.String()), 0o644); err != nil {
			fmt.Fprintln(os.Stderr, "fatal:", err)
			os.Exit(1)
		}
		totalBytes += int64(sb.Len())
		fmt.Printf("wrote %s (%s)\n", path, human(int64(sb.Len())))
	}
	// edge cases
	_ = os.WriteFile(filepath.Join(*out, "tiny.txt"), []byte("Hello world. Hello MapReduce!\nhello femboys of the world\n"), 0o644)
	_ = os.WriteFile(filepath.Join(*out, "empty.txt"), nil, 0o644)
	// unicode + punctuation torture test
	var uni strings.Builder
	uniLines := []string{
		"Café mornings are naïve pleasures, aren't they?",
		"The well-known e-mail arrived — résumé attached.",
		"'Tis a tschüß from Zürich: 100% über-cool.",
		"Don't stop-believin', hold on to that feelin'!",
		"Ünïcödé wörds lïke thësë ärë rärëly üsed.",
	}
	for i := 0; i < 600; i++ {
		uni.WriteString(uniLines[i%len(uniLines)])
		uni.WriteString("\n")
	}
	_ = os.WriteFile(filepath.Join(*out, "unicode_letters.txt"), []byte(uni.String()), 0o644)
	totalBytes += int64(uni.Len())
	fmt.Printf("wrote %s (%s)\n", filepath.Join(*out, "unicode_letters.txt"), human(int64(uni.Len())))
	fmt.Printf("total mock data: %s across %d files (+ tiny.txt, empty.txt)\n", human(totalBytes), len(books)+3)
}

func human(n int64) string {
	switch {
	case n >= 1<<20:
		return fmt.Sprintf("%.1fMB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1fkB", float64(n)/(1<<10))
	default:
		return fmt.Sprintf("%dB", n)
	}
}
