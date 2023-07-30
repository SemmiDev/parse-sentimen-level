package main

import (
	"encoding/csv"
	"log"
	"os"
	"runtime"
	"sync"
	"time"
)

var sentimenMapper = map[string][]string{
	"1": {"-1", "Negatif"},
	"2": {"-1", "Negatif"},
	"3": {"0", "Netral"},
	"4": {"1", "Positif"},
	"5": {"1", "Positif"},
}

type Review struct {
	Content    string
	Rating     string // we just store as string, for minimizing conversion
	CategoryLv string
	Category   string
}

func main() {
	inputFileName := "threads_app_reviews.csv"
	outputFileName := "output.csv"

	start := time.Now()

	reviews, err := readCSV(inputFileName)
	if err != nil {
		log.Fatalf("Error reading CSV: %v", err)
	}

	resultCh := processReviews(reviews)

	if err := writeCSV(outputFileName, resultCh); err != nil {
		log.Fatalf("Error writing CSV: %v", err)
	}

	log.Println("Done in:", time.Since(start))
}

func readCSV(fileName string) ([]Review, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	_, err = reader.Read()
	if err != nil {
		return nil, err
	}

	reviews := []Review{}
	for {
		record, err := reader.Read()
		if err != nil {
			break
		}

		review := Review{
			Content: record[0],
			Rating:  record[1],
		}

		reviews = append(reviews, review)
	}

	return reviews, nil
}

func processReviews(reviews []Review) <-chan Review {
	resultCh := make(chan Review)
	var wg sync.WaitGroup

	numCPU := runtime.NumCPU()
	log.Println("Total Worker:", numCPU)

	wg.Add(numCPU)

	for i := 0; i < numCPU; i++ {
		go func() {
			defer wg.Done()
			for _, review := range reviews {
				mapper := sentimenMapper[review.Rating]
				review.CategoryLv = mapper[0]
				review.Category = mapper[1]
				resultCh <- review
			}
		}()
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	return resultCh
}

func writeCSV(fileName string, reviews <-chan Review) error {
	file, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	if err := writer.Write([]string{"Content", "Rating", "Category Level", "Category"}); err != nil {
		return err
	}

	for review := range reviews {
		row := []string{
			review.Content,
			review.Rating,
			review.CategoryLv,
			review.Category,
		}
		if err := writer.Write(row); err != nil {
			return err
		}
	}

	return nil
}
