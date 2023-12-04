package bucket

type Message struct {
	Id      string `json:"id"`
	Message string `json:"message"`
}

type Messages []Message
