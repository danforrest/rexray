package isilon

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	//	"reflect"

	"strconv"
	"strings"

	isi "github.com/emccode/goisilon"

	log "github.com/Sirupsen/logrus"
	"github.com/akutz/gofig"
	"github.com/akutz/goof"

	"github.com/emccode/rexray/core"
	"github.com/emccode/rexray/core/errors"
)

const providerName = "Isilon"

// The Isilon storage driver.
type driver struct {
	client *isi.Client
	r      *core.RexRay
}

func ef() goof.Fields {
	return goof.Fields{
		"provider": providerName,
	}
}

func eff(fields goof.Fields) map[string]interface{} {
	errFields := map[string]interface{}{
		"provider": providerName,
	}
	if fields != nil {
		for k, v := range fields {
			errFields[k] = v
		}
	}
	return errFields
}

func init() {
	core.RegisterDriver(providerName, newDriver)
	gofig.Register(configRegistration())
}

func newDriver() core.Driver {
	return &driver{}
}

func (d *driver) Init(r *core.RexRay) error {

	d.r = r

	fields := eff(map[string]interface{}{
		"endpoint":   d.endpoint(),
		"userName":   d.userName(),
		"insecure":   d.insecure(),
		"volumePath": d.volumePath(),
	})

	if d.password() == "" {
		fields["password"] = ""
	} else {
		fields["password"] = "******"
	}

	if !isIsilonAttached() {
		return goof.WithFields(fields, "device not detected")
	}

	var err error

	if d.client, err = isi.NewClientWithArgs(
		d.endpoint(),
		d.insecure(),
		d.userName(),
		d.password(),
		d.volumePath()); err != nil {
		return goof.WithFieldsE(fields,
			"error creating isilon client", err)
	}

	log.WithField("provider", providerName).Info("storage driver initialized")

	return nil
}

var scsiDeviceVendors []string

func walkDevices(path string, f os.FileInfo, err error) error {
	vendorFilePath := fmt.Sprintf("%s/device/vendor", path)
	// fmt.Printf("vendorFilePath: %+v\n", string(vendorFilePath))
	data, _ := ioutil.ReadFile(vendorFilePath)
	scsiDeviceVendors = append(scsiDeviceVendors, strings.TrimSpace(string(data)))
	return nil
}

var isIsilonAttached = func() bool {
	return true
	filepath.Walk("/sys/class/scsi_device/", walkDevices)
	for _, vendor := range scsiDeviceVendors {
		if vendor == "Isilon" {
			return true
		}
	}
	return false
}

func (d *driver) Name() string {
	return providerName
}

func (d *driver) GetInstance() (*core.Instance, error) {
	log.Println("Start GetInstance()")

	ipList, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}
	log.Println("address list:", ipList)
	id := ""
	for _, addr := range ipList {
		if strings.Contains(addr.String(), "127.0.0.1/8") == false {
			id = addr.String()
			break
		}
	}

	instance := &core.Instance{
		ProviderName: providerName,
		InstanceID:   id,
		Region:       "",
		Name:         "",
	}

	log.Println("Got Instance: " + fmt.Sprintf("%+v", instance))
	return instance, nil
}

func (d *driver) nfsMountPath(mountPath string) string {
	return fmt.Sprintf("%s:%s", d.nfsHost(), mountPath)
}

func (d *driver) GetVolumeMapping() ([]*core.BlockDevice, error) {
	log.Println("Start GetVolumeMapping()")

	exports, err := d.client.GetVolumeExports()
	if err != nil {
		return nil, err
	}

	var BlockDevices []*core.BlockDevice
	for _, export := range exports {
		device := &core.BlockDevice{
			ProviderName: providerName,
			InstanceID:   "",
			Region:       "",
			DeviceName:   d.nfsMountPath(export.ExportPath),
			VolumeID:     export.Volume.Name,
			NetworkName:  export.ExportPath,
			Status:       "",
		}
		BlockDevices = append(BlockDevices, device)
	}

	return BlockDevices, nil
}

func (d *driver) getVolume(volumeID, volumeName string) ([]isi.Volume, error) {
	var volumes []isi.Volume
	if volumeID != "" || volumeName != "" {
		volume, err := d.client.GetVolume(volumeID, volumeName)
		if err != nil && !strings.Contains(err.Error(), "Unable to open object") {
			return nil, err
		}
		if volume != nil {
			volumes = append(volumes, volume)
		}
	} else {
		var err error
		volumes, err = d.client.GetVolumes()
		if err != nil {
			return nil, err
		}

	}
	return volumes, nil
}

func (d *driver) getSize(volumeID, volumeName string) (int64, error) {

	if volumeID != "" || volumeName != "" {
		quota, err := d.client.GetQuota(volumeID, volumeName)
		if err != nil {
			return 0, nil
		}

		return quota.Thresholds.Hard, nil
	}

	return 0, errors.ErrMissingVolumeID

}

func (d *driver) GetVolume(volumeID, volumeName string) ([]*core.Volume, error) {
	log.Println("Start GetVolume()")

	volumes, err := d.getVolume(volumeID, volumeName)
	if err != nil {
		return nil, err
	}

	if len(volumes) == 0 {
		return nil, nil
	}

	localVolumeMappings, err := d.GetVolumeMapping()
	if err != nil {
		return nil, err
	}

	blockDeviceMap := make(map[string]*core.BlockDevice)
	for _, volume := range localVolumeMappings {
		blockDeviceMap[volume.VolumeID] = volume
	}

	var volumesSD []*core.Volume
	for _, volume := range volumes {
		var attachmentsSD []*core.VolumeAttachment
		if _, exists := blockDeviceMap[volume.Name]; exists {
			instance, _ := d.GetInstance()
// TODO: Should the instance id be set here??? 			
			attachmentSD := &core.VolumeAttachment{
				VolumeID: volume.Name,
				InstanceID: instance.InstanceID,
				DeviceName: blockDeviceMap[volume.Name].DeviceName,
				Status:     "",
			}
			attachmentsSD = append(attachmentsSD, attachmentSD)
		}

		volSize, _ := d.getSize(volume.Name, volume.Name)

		volumeSD := &core.Volume{
			Name:             volume.Name,
			VolumeID:         volume.Name,
			Size:             strconv.FormatInt(volSize/1024/1024, 10),
			AvailabilityZone: "",
			NetworkName:      d.client.Path(volume.Name),
			Attachments:      attachmentsSD,
		}
		volumesSD = append(volumesSD, volumeSD)
	}

	return volumesSD, nil
}

func (d *driver) CreateVolume(
	notUsed bool,
	volumeName, volumeID, snapshotID, NUvolumeType string,
	NUIOPS, size int64, NUavailabilityZone string) (*core.Volume, error) {
	log.Println("Start CreateVolume() (", volumeName, ") (", volumeID, ")")

	d.client.CreateVolume(volumeName)

	err := d.client.SetQuota(volumeName, size)
	if err != nil {
		// TODO: not sure how to handle this situation.  Delete created volume
		// and return an error?  Ignore and continue?
	}

	volumes, _ := d.GetVolume(volumeID, volumeName)

	return volumes[0], nil
}

func (d *driver) RemoveVolume(volumeID string) error {
	err := d.client.DeleteVolume(volumeID)
	if err != nil {
		return err
	}

	return nil
}

//GetSnapshot returns snapshots from a volume or a specific snapshot
func (d *driver) GetSnapshot(
	volumeID, snapshotID, snapshotName string) ([]*core.Snapshot, error) {
	return nil, nil
}

func getIndex(href string) string {
	hrefFields := strings.Split(href, "/")
	return hrefFields[len(hrefFields)-1]
}

func (d *driver) CreateSnapshot(
	notUsed bool,
	snapshotName, volumeID, description string) ([]*core.Snapshot, error) {
	return nil, nil
}

func (d *driver) RemoveSnapshot(snapshotID string) error {
	return nil
}

func (d *driver) GetVolumeAttach(volumeID, instanceID string) ([]*core.VolumeAttachment, error) {
	log.Println("Start GetVolumeAttach()")
	if volumeID == "" {
		return []*core.VolumeAttachment{}, errors.ErrMissingVolumeID
	}
	volume, err := d.GetVolume(volumeID, "")
	if err != nil {
		return []*core.VolumeAttachment{}, err
	}

	if instanceID != "" {
		var attached bool
		for _, volumeAttachment := range volume[0].Attachments {
			if volumeAttachment.InstanceID == instanceID {
				return volume[0].Attachments, nil
			}
		}
		if !attached {
			return []*core.VolumeAttachment{}, nil
		}
	}
	return volume[0].Attachments, nil
}

func (d *driver) AttachVolume(
	notused bool,
	volumeID, instanceID string, force bool) ([]*core.VolumeAttachment, error) {
	log.Println("Start AttachVolume(): ")
	log.Println("vol id: ", volumeID)
	log.Println("inst id: ", instanceID)

	if volumeID == "" {
		return nil, errors.ErrMissingVolumeID
	}

	volumes, err := d.GetVolume(volumeID, "")
	if err != nil {
		return nil, err
	}

	if len(volumes) == 0 {
		return nil, errors.ErrNoVolumesReturned
	}

	if err := d.client.ExportVolume(volumeID); err != nil {
		return nil, goof.WithError("problem exporting volume", err)
	}
	clients, err := d.client.GetExportClients(volumeID)
	if err != nil {
		return nil, goof.WithError("problem getting export client", err)
	}
	if clients != nil {
		for _, client := range clients {
			log.Println("client: ", client)
		}
	}
	// clear out any existing clients.  if force is false and we have existing clients, we need to exit early
	
	// TODO: This is setting the instance id (via GetVolume) regardless of if the volume is attached or not
	volumeAttachment, err := d.GetVolumeAttach(volumeID, instanceID)
	for _, att := range volumeAttachment {
		log.Println("volume attachment: ", att)
	}
	if err != nil {
		return nil, err
	}

	// TODO: 
	if err := d.client.SetExportClient(volumeID, instanceID); err != nil {
		return nil, goof.WithError("problem setting export client", err)
	}

	return volumeAttachment, nil

}

func (d *driver) DetachVolume(notUsed bool, volumeID string, blank string, force bool) error {
	log.Println("Start DetachVolume()")
	if volumeID == "" {
		return errors.ErrMissingVolumeID
	}

	volumes, err := d.GetVolume(volumeID, "")
	if err != nil {
		return err
	}

	if len(volumes) == 0 {
		return errors.ErrNoVolumesReturned
	}

	if err := d.client.UnexportVolume(volumeID); err != nil {
		return goof.WithError("problem unexporting volume", err)
	}

	log.Println("Detached volume", volumeID)
	return nil
}

func (d *driver) CopySnapshot(
	runAsync bool,
	volumeID, snapshotID, snapshotName,
	destinationSnapshotName, destinationRegion string) (*core.Snapshot, error) {
	log.Println("Start CopySnapshot()")
	return nil, errors.ErrNotImplemented
}

func (d *driver) GetDeviceNextAvailable() (string, error) {
	log.Println("Start GetDeviceNextAvailable()")
	return "", errors.ErrNotImplemented
}

func (d *driver) endpoint() string {
	return d.r.Config.GetString("isilon.endpoint")
}

func (d *driver) insecure() bool {
	return d.r.Config.GetBool("isilon.insecure")
}

func (d *driver) userName() string {
	return d.r.Config.GetString("isilon.userName")
}

func (d *driver) password() string {
	return d.r.Config.GetString("isilon.password")
}

func (d *driver) volumePath() string {
	return d.r.Config.GetString("isilon.volumePath")
}

func (d *driver) nfsHost() string {
	return d.r.Config.GetString("isilon.nfsHost")
}

func configRegistration() *gofig.Registration {
	r := gofig.NewRegistration("Isilon")
	r.Key(gofig.String, "", "", "", "isilon.endpoint")
	r.Key(gofig.Bool, "", false, "", "isilon.insecure")
	r.Key(gofig.String, "", "", "", "isilon.userName")
	r.Key(gofig.String, "", "", "", "isilon.password")
	r.Key(gofig.String, "", "", "", "isilon.volumePath")
	r.Key(gofig.String, "", "", "", "isilon.nfsHost")
	return r
}
