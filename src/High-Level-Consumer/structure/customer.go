package customer

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