package providers

import (
	"github.com/kube-project/image-processor-service/pkg/models"
)

// ImageStorer handles storing and updating images for the image processor.
//
//go:generate counterfeiter -o fakes/fake_storer.go . ImageStorer
type ImageStorer interface {
	GetImage(id int) (*models.Image, error)
	UpdateImage(id int, person int, status models.Status) error
	GetPersonFromImage(image string) (*models.Person, error)
}
