package main

import (
	"context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pb "kvraft/src/grpc/server/proto"
)

func main() {
	conn, _ := grpc.Dial("localhost:8080", grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer conn.Close()
	client := pb.NewKVServerClient(conn)
	vote, _ := client.RequestVote(context.Background(), &pb.RequestVoteArgs{Me: 1})
	println(vote.Success)
}
