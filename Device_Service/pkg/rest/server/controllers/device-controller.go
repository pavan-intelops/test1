package controllers

import (
	"github.com/gin-gonic/gin"
	"github.com/pavan-intelops/test1/device_service/pkg/rest/server/models"
	"github.com/pavan-intelops/test1/device_service/pkg/rest/server/services"
	log "github.com/sirupsen/logrus"
	"net/http"
	"strconv"
)

type DeviceController struct {
	deviceService *services.DeviceService
}

func NewDeviceController() (*DeviceController, error) {
	deviceService, err := services.NewDeviceService()
	if err != nil {
		return nil, err
	}
	return &DeviceController{
		deviceService: deviceService,
	}, nil
}

func (deviceController *DeviceController) CreateDevice(context *gin.Context) {
	// validate input
	var input models.Device
	if err := context.ShouldBindJSON(&input); err != nil {
		log.Error(err)
		context.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// trigger device creation
	if _, err := deviceController.deviceService.CreateDevice(&input); err != nil {
		log.Error(err)
		context.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	context.JSON(http.StatusCreated, gin.H{"message": "Device created successfully"})
}

func (deviceController *DeviceController) UpdateDevice(context *gin.Context) {
	// validate input
	var input models.Device
	if err := context.ShouldBindJSON(&input); err != nil {
		log.Error(err)
		context.JSON(http.StatusUnprocessableEntity, gin.H{"error": err.Error()})
		return
	}

	id, err := strconv.ParseInt(context.Param("id"), 10, 64)
	if err != nil {
		log.Error(err)
		context.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// trigger device update
	if _, err := deviceController.deviceService.UpdateDevice(id, &input); err != nil {
		log.Error(err)
		context.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	context.JSON(http.StatusOK, gin.H{"message": "Device updated successfully"})
}

func (deviceController *DeviceController) FetchDevice(context *gin.Context) {
	id, err := strconv.ParseInt(context.Param("id"), 10, 64)
	if err != nil {
		log.Error(err)
		context.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// trigger device fetching
	device, err := deviceController.deviceService.GetDevice(id)
	if err != nil {
		log.Error(err)
		context.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	context.JSON(http.StatusOK, device)
}

func (deviceController *DeviceController) DeleteDevice(context *gin.Context) {
	id, err := strconv.ParseInt(context.Param("id"), 10, 64)
	if err != nil {
		log.Error(err)
		context.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// trigger device deletion
	if err := deviceController.deviceService.DeleteDevice(id); err != nil {
		log.Error(err)
		context.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	context.JSON(http.StatusOK, gin.H{
		"message": "Device deleted successfully",
	})
}

func (deviceController *DeviceController) ListDevices(context *gin.Context) {
	// trigger all devices fetching
	devices, err := deviceController.deviceService.ListDevices()
	if err != nil {
		log.Error(err)
		context.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	context.JSON(http.StatusOK, devices)
}

func (*DeviceController) PatchDevice(context *gin.Context) {
	context.JSON(http.StatusOK, gin.H{
		"message": "PATCH",
	})
}

func (*DeviceController) OptionsDevice(context *gin.Context) {
	context.JSON(http.StatusOK, gin.H{
		"message": "OPTIONS",
	})
}

func (*DeviceController) HeadDevice(context *gin.Context) {
	context.JSON(http.StatusOK, gin.H{
		"message": "HEAD",
	})
}
