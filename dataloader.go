package dataloader

type Table struct {
	header TableHeader
	rows   [][]string
}

func (table *Table) Header() TableHeader {
	return table.header
}

func (table *Table) Rows() [][]string {
	return table.rows
}

func NewDataTable(data [][]string) *Table {
	header := data[0]
	rows := data[1:]
	return &Table{
		header: NewTableHeader(header),
		rows:   rows,
	}
}

type TableHeader map[string]int

func NewTableHeader(labels []string) TableHeader {
	lblIdx := make(TableHeader)
	for idx, lbl := range labels {
		lblIdx[lbl] = idx + 1
	}
	return lblIdx
}

func (lblIdx TableHeader) getLabel(label string) int {
	return lblIdx[label] - 1
}

type RLoader struct {
	header TableHeader
	data   []string

	defaultVal string
}

func NewRLoader(header TableHeader, data []string) *RLoader {
	return &RLoader{
		header:     header,
		data:       data,
		defaultVal: "",
	}
}

func (loader *RLoader) WithDefault(defaultVal string) *RLoader {
	if loader == nil {
		return loader
	}
	res := *loader
	res.defaultVal = defaultVal
	return &res
}

func (loader *RLoader) Load(label string) string {
	colIdx := loader.header.getLabel(label)
	if colIdx == -1 || colIdx >= len(loader.data) {
		return loader.defaultVal
	}
	return loader.data[colIdx]
}

type TLoader[T comparable] struct {
	*RLoader
	parser     func(s string) T
	defaultVal T
}

func NewTLoader[T comparable](rLoader *RLoader, parser func(s string) T) *TLoader[T] {
	return &TLoader[T]{
		RLoader: rLoader,
		parser:  parser,
	}
}

func (loader *TLoader[T]) WithDefault(defaultVal T) *TLoader[T] {
	if loader == nil {
		return &TLoader[T]{}
	}
	res := *loader
	res.defaultVal = defaultVal
	return &res
}

func (loader *TLoader[T]) Load(label string) T {
	rVal := loader.RLoader.Load(label)
	val := loader.parser(rVal)
	var d T
	if val == d {
		val = loader.defaultVal
	}
	return val
}
