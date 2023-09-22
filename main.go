package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
)

const (
	dataDir        = "data/"
	outputDir      = "output/"
	outputFileType = ".txt"
)

func main() {
	pwd, _ := os.Getwd()
	err := cleanUpOutputDir(pwd)
	if err != nil {
		panic(err)
	}

	files, err := os.Open(dataDir)
	if err != nil {
		log.Fatal(err)
	}

	defer files.Close()

	fileNames, err := files.Readdirnames(0)

	if err != nil {
		panic(err)
	}

	for _, file := range fileNames {
		fmt.Printf("Reading %s \n", file)
		// skip dir no recursion
		fileInfo, err := os.Stat(filepath.Join(pwd, dataDir, file))
		if err != nil {
			panic(err)
		}

		if fileInfo.IsDir() {
			fmt.Printf("%s is directory\n", file)
			continue
		}

		var nameArr = strings.Split(file, ".")

		// skip non-csv file
		if nameArr[len(nameArr)-1] != "csv" {
			fmt.Printf("%s is not csv file\n", file)
			continue
		}

		tableName := nameArr[0]

		csvPath := filepath.Join(pwd, dataDir+file)
		rawData := readCSV(csvPath)

		processData(rawData, tableName)
	}
}

func cleanUpOutputDir(pwd string) error {

	fmt.Println("--- Start Clean Up Output Directory ---")
	files, err := os.Open(outputDir)

	if err != nil {
		return err
	}

	defer files.Close()

	fileNames, err := files.Readdirnames(0)

	if err != nil {
		return err
	}

	for _, name := range fileNames {
		fmt.Printf("Cleaning Up %s\n", name)
		err := os.RemoveAll(filepath.Join(pwd, outputDir, name))
		if err != nil {
			panic(err)
		}
	}

	fmt.Println("--- Finish Clean Up Output Directory ---")
	return nil
}

func readCSV(path string) [][]string {
	// open csv
	fd, error := os.Open(path)

	if error != nil {
		fmt.Println(error)
	}

	fmt.Println("Open csv successfully")
	defer fd.Close()

	// read csv
	fileReader := csv.NewReader(fd)
	records, error := fileReader.ReadAll()
	if error != nil {
		fmt.Println(error)
	}
	return records
}

func processData(rawData [][]string, table string) {
	var sb strings.Builder
	sb.WriteString("INSERT INTO ")
	sb.WriteString(table)
	sb.WriteString(" VALUES ")

	if len(rawData) < 1 {
		fmt.Printf("No record for %s\n", table)
		return
	}
	dataEntry := make([]string, len(rawData)-1)
	for index, row := range rawData {
		if index == 0 {
			// first line consider as col name, auto skip
			continue
		}

		var rowSb strings.Builder
		rowSb.WriteString("(")

		rowData := make([]string, len(row)-1)
		for colIndex, col := range row {
			if colIndex == 0 {
				// first col consider as id, auto skip
				continue
			}
			var tempSb strings.Builder
			tempSb.WriteString("'")
			tempSb.WriteString(col)
			tempSb.WriteString("'")

			rowData[colIndex-1] = tempSb.String()
		}
		rowSb.WriteString(strings.Join(rowData, ", "))
		rowSb.WriteString(")")
		dataEntry[index-1] = rowSb.String()
	}

	sb.WriteString(strings.Join(dataEntry, ", "))

	fmt.Println(sb.String())

	outputFile := table + outputFileType

	// save to output dir
	file, err := os.Create(filepath.Join(outputDir, outputFile))

	if err != nil {
		panic(err)
	}

	defer file.Close()

	bw := bufio.NewWriter(file)

	_, err = bw.WriteString(sb.String())

	if err != nil {
		panic(err)
	}

	bw.Flush()

	fmt.Printf("Wrote into %s \n", outputFile)
}
