package structures

// type Tasks struct {
// 	Task []Task `json:"tasks"`
// }

type Block struct {
	Id string `json:"id"`
	Value string `json:"value"`
}

type Person struct {
	Name string `json:"name"`
	Account string `json:"account"`
}

type Task struct {
    CreatedAt string	`json:"createdAt"`
    Name string			`json:"name"`
    Avatar string		`json:"avatar"`
	Agee int32 			`json:"agee"`
	Completed bool 		`json:"completed"`
	Id string 			`json:"id"`
}

type Street struct {
	Name string `json:"name"`
	Address string `json:"address"`
}

type Customers struct {
	Customer []Customer `json:"customers"`
}

type Customer struct {
    Name string	`json:"name"`
    Age int32	`json:"age"`
    Items Items	`json:"items"`
}

type Items struct {
	Snack string `json:"snack"`
	Fruit string `json:"fruit"`
}