package main

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// Data Structures

type User struct {
	ID        int
	Username  string
	Karma     int
	Actions   int
	Connected bool
}

type SubReddit struct {
	Name  string
	Posts []Post
	Users map[int]*User
}

type Post struct {
	ID       int
	Author   *User
	Content  string
	Comments []Comment
	Votes    int
}

type Comment struct {
	ID      int
	Author  *User
	Content string
	Replies []Comment
	Votes   int
}

type Message struct {
	From    *User
	To      *User
	Content string
}

type Engine struct {
	Users             map[int]*User
	SubReddits        map[string]*SubReddit
	Messages          []Message
	PostID            int
	CommentID         int
	TotalPosts        int
	TotalVotes        int
	TotalMessages     int
	TotalActions      int
	TotalComments     int
	DisconnectedUsers int
	StartTime         time.Time
	Mutex             sync.Mutex
	ActionBreakdown   map[string]int
}

// Initialization and Utility Functions

func NewEngine() *Engine {
	return &Engine{
		Users:      make(map[int]*User),
		SubReddits: make(map[string]*SubReddit),
		Messages:   []Message{},
		PostID:     1,
		CommentID:  1,
		StartTime:  time.Now(),
		ActionBreakdown: map[string]int{
			"Posts":    0,
			"Comments": 0,
			"Votes":    0,
			"Messages": 0,
		},
	}
}

func (e *Engine) RegisterUser(username string) *User {
	e.Mutex.Lock()
	defer e.Mutex.Unlock()
	id := len(e.Users) + 1
	user := &User{ID: id, Username: username, Karma: 0, Actions: 0, Connected: true}
	e.Users[id] = user
	return user
}

func (e *Engine) CreateSubReddit(name string) *SubReddit {
	e.Mutex.Lock()
	defer e.Mutex.Unlock()
	if _, exists := e.SubReddits[name]; exists {
		return nil
	}
	subReddit := &SubReddit{Name: name, Posts: []Post{}, Users: make(map[int]*User)}
	e.SubReddits[name] = subReddit
	return subReddit
}

func (e *Engine) JoinSubReddit(user *User, subRedditName string) bool {
	e.Mutex.Lock()
	defer e.Mutex.Unlock()
	subReddit, exists := e.SubReddits[subRedditName]
	if !exists {
		return false
	}
	subReddit.Users[user.ID] = user
	user.Actions++
	e.TotalActions++
	return true
}

func (e *Engine) LeaveSubReddit(user *User, subRedditName string) bool {
	e.Mutex.Lock()
	defer e.Mutex.Unlock()
	subReddit, exists := e.SubReddits[subRedditName]
	if !exists {
		return false
	}
	delete(subReddit.Users, user.ID)
	user.Actions++
	e.TotalActions++
	return true
}

func (e *Engine) CreatePost(user *User, subRedditName, content string) *Post {
	e.Mutex.Lock()
	defer e.Mutex.Unlock()
	subReddit, exists := e.SubReddits[subRedditName]
	if !exists {
		return nil
	}
	post := Post{ID: e.PostID, Author: user, Content: content, Comments: []Comment{}, Votes: 0}
	e.PostID++
	e.TotalPosts++
	e.ActionBreakdown["Posts"]++
	user.Actions++
	e.TotalActions++
	subReddit.Posts = append(subReddit.Posts, post)
	return &post
}

func (e *Engine) CreateRepost(user *User, originalPost *Post, subRedditName string) *Post {
	e.Mutex.Lock()
	defer e.Mutex.Unlock()
	subReddit, exists := e.SubReddits[subRedditName]
	if !exists {
		return nil
	}
	repost := Post{ID: e.PostID, Author: user, Content: originalPost.Content, Comments: []Comment{}, Votes: 0}
	e.PostID++
	e.TotalPosts++
	e.ActionBreakdown["Posts"]++
	user.Actions++
	e.TotalActions++
	subReddit.Posts = append(subReddit.Posts, repost)
	return &repost
}

func (e *Engine) CommentPost(user *User, post *Post, content string) *Comment {
	e.Mutex.Lock()
	defer e.Mutex.Unlock()
	comment := Comment{ID: e.CommentID, Author: user, Content: content, Replies: []Comment{}, Votes: 0}
	e.CommentID++
	post.Comments = append(post.Comments, comment)
	e.TotalComments++
	e.ActionBreakdown["Comments"]++
	user.Actions++
	e.TotalActions++
	return &comment
}

func (e *Engine) AddReplyToComment(user *User, parentComment *Comment, content string) *Comment {
	e.Mutex.Lock()
	defer e.Mutex.Unlock()
	reply := Comment{ID: e.CommentID, Author: user, Content: content, Replies: []Comment{}, Votes: 0}
	e.CommentID++
	parentComment.Replies = append(parentComment.Replies, reply)
	e.TotalComments++
	e.ActionBreakdown["Comments"]++
	user.Actions++
	e.TotalActions++
	return &reply
}

func (e *Engine) UpvotePost(post *Post) {
	e.Mutex.Lock()
	defer e.Mutex.Unlock()
	post.Votes++
	post.Author.Karma++
	e.TotalVotes++
	e.ActionBreakdown["Votes"]++
	e.TotalActions++
}

func (e *Engine) DownvotePost(post *Post) {
	e.Mutex.Lock()
	defer e.Mutex.Unlock()
	post.Votes--
	post.Author.Karma--
	e.TotalVotes++
	e.ActionBreakdown["Votes"]++
	e.TotalActions++
}

func (e *Engine) SendDirectMessage(from, to *User, content string) {
	e.Mutex.Lock()
	defer e.Mutex.Unlock()
	message := Message{From: from, To: to, Content: content}
	e.Messages = append(e.Messages, message)
	e.TotalMessages++
	e.ActionBreakdown["Messages"]++
	from.Actions++
	e.TotalActions++
}

func (e *Engine) RetrieveMessages(user *User) []Message {
	e.Mutex.Lock()
	defer e.Mutex.Unlock()
	var userMessages []Message
	for _, message := range e.Messages {
		if message.To == user {
			userMessages = append(userMessages, message)
		}
	}
	return userMessages
}

func (e *Engine) ReplyToMessage(user *User, original Message, content string) {
	e.SendDirectMessage(user, original.From, content)
}

func (e *Engine) GetUserFeed(user *User) []Post {
	e.Mutex.Lock()
	defer e.Mutex.Unlock()
	var feed []Post
	for _, subreddit := range e.SubReddits {
		if _, subscribed := subreddit.Users[user.ID]; subscribed {
			feed = append(feed, subreddit.Posts...)
		}
	}
	return feed
}

// Simulator Functions

func simulateUsers(engine *Engine, numUsers int, numSubReddits int) {
	// Create subreddits
	for i := 0; i < numSubReddits; i++ {
		subRedditName := fmt.Sprintf("SubReddit%d", i+1)
		engine.CreateSubReddit(subRedditName)
	}

	for i := 0; i < numUsers; i++ {
		username := fmt.Sprintf("User%d", i+1)
		user := engine.RegisterUser(username)
		subCount := int(float64(numSubReddits)*math.Pow(rand.Float64(), 1.2)) + 1
		for j := 0; j < subCount && j < numSubReddits; j++ {
			subRedditName := fmt.Sprintf("SubReddit%d", j+1)
			engine.JoinSubReddit(user, subRedditName)
		}

		// Randomly disconnect/connect users
		if rand.Float64() > 0.2 {
			user.Connected = true
		} else {
			user.Connected = false
			engine.DisconnectedUsers++
		}

		// Create posts and comments
		for j := 0; j < rand.Intn(3)+1; j++ {
			if user.Connected {
				post := engine.CreatePost(user, fmt.Sprintf("SubReddit%d", rand.Intn(numSubReddits)+1), fmt.Sprintf("Post content %d from %s", j+1, username))
				if post != nil {
					for k := 0; k < rand.Intn(3)+1; k++ {
						engine.UpvotePost(post)
					}
					// Simulate comments on posts
					for l := 0; l < rand.Intn(2)+1; l++ {
						comment := engine.CommentPost(user, post, fmt.Sprintf("Comment %d on post %d", l+1, post.ID))
						for m := 0; m < rand.Intn(2)+1; m++ {
							engine.AddReplyToComment(user, comment, fmt.Sprintf("Reply %d to comment %d", m+1, comment.ID))
						}
					}
					// Simulate reposts
					if rand.Float64() < 0.1 {
						engine.CreateRepost(user, post, fmt.Sprintf("SubReddit%d", rand.Intn(numSubReddits)+1))
					}
				}
			}
		}

		// Simulate direct messages
		if rand.Float64() < 0.2 && len(engine.Users) > 1 {
			targetUserID := rand.Intn(len(engine.Users)) + 1
			if targetUserID != user.ID {
				targetUser := engine.Users[targetUserID]
				engine.SendDirectMessage(user, targetUser, fmt.Sprintf("Hello from %s to %s!", user.Username, targetUser.Username))
			}
		}
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())
	engine := NewEngine()

	// Simulate users and subreddits
	numUsers := 100
	numSubReddits := 10
	simulateUsers(engine, numUsers, numSubReddits)

	// Calculate throughput
	duration := time.Since(engine.StartTime).Seconds()
	throughput := float64(engine.TotalActions) / duration

	fmt.Println("Simulation Complete. Metrics:")
	fmt.Printf("Users: %d\n", len(engine.Users))
	fmt.Printf("SubReddits: %d\n", len(engine.SubReddits))
	fmt.Printf("Total Posts: %d\n", engine.TotalPosts)
	fmt.Printf("Total Votes: %d\n", engine.TotalVotes)
	fmt.Printf("Total Comments: %d\n", engine.TotalComments)
	fmt.Printf("Total Messages: %d\n", engine.TotalMessages)
	fmt.Printf("Total Actions: %d\n", engine.TotalActions)
	fmt.Printf("Throughput (actions/sec): %.2f\n", throughput)
	fmt.Printf("Disconnected Users: %d\n", engine.DisconnectedUsers)

	// Display Action Breakdown
	fmt.Println("Action Breakdown:")
	for action, count := range engine.ActionBreakdown {
		fmt.Printf("%s: %d\n", action, count)
	}

	// Display Subreddit Metrics
	fmt.Println("\nSubReddit Metrics (Zipf Distribution Impact):")
	type SubRedditStats struct {
		Name      string
		Members   int
		PostCount int
	}
	var subredditStats []SubRedditStats
	for name, subreddit := range engine.SubReddits {
		stats := SubRedditStats{
			Name:      name,
			Members:   len(subreddit.Users),
			PostCount: len(subreddit.Posts),
		}
		subredditStats = append(subredditStats, stats)
	}

	sort.Slice(subredditStats, func(i, j int) bool {
		return subredditStats[i].Members > subredditStats[j].Members
	})

	for i, stats := range subredditStats {
		fmt.Printf("%d. %s - Members: %d, Posts: %d\n", i+1, stats.Name, stats.Members, stats.PostCount)
	}

	// Display Random User Feed
	fmt.Println("\nFeed for a Random User:")
	randomUser := engine.Users[rand.Intn(len(engine.Users))+1]
	feed := engine.GetUserFeed(randomUser)
	for _, post := range feed {
		fmt.Printf("Post ID %d by %s: %s\n", post.ID, post.Author.Username, post.Content)
	}

	// Display Direct Messages Metrics
	fmt.Println("\nDirect Messages:")
	for _, message := range engine.Messages {
		fmt.Printf("From %s to %s: %s\n", message.From.Username, message.To.Username, message.Content)
	}
}
