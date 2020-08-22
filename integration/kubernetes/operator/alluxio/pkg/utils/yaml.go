package utils

import (
	"os"

	"github.com/go-logr/logr"
	ctrl "sigs.k8s.io/controller-runtime"

	yaml "gopkg.in/yaml.v2"
)

var log logr.Logger

func init() {
	log = ctrl.Log.WithName("utils")
}

func ToYaml(values interface{}, file *os.File) error {
	log.V(1).Info("create yaml file", "values", values)
	data, err := yaml.Marshal(values)
	if err != nil {
		log.Error(err, "failed to marshal value", "value", values)
		return err
	}

	defer file.Close()
	_, err = file.Write(data)
	if err != nil {
		log.Error(err, "failed to write file", "data", data, "fileName", file.Name())
	}
	return err
}
