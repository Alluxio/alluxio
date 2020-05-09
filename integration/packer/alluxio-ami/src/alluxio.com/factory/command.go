package factory

import (
	"fmt"
	"log"
	"os/exec"
	"strings"
	"time"
)

func Exec(app string, args []string) (string, error) {
	log.Println(fmt.Sprintf("%v %v", app, strings.Join(args, " ")))
	cmd := exec.Command(app, args...)
	stdout, err := cmd.Output()

	if err != nil {
		return "", err
	}
	return string(stdout), nil
}

// RetryWithWait retries until the condition is true or the max attempts is hit, sleeping the given duration in between each attempt
func RetryWithWait(duration time.Duration, attempts int, condition func() (bool, error)) error {
	for i := 0; i < attempts; i++ {
		done, err := condition()
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		time.Sleep(duration)
	}
	return fmt.Errorf("retries failed after %d attempts", attempts)
}
