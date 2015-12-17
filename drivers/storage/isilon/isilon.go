package isilon

import (
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"

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
const bytesPerGb = int64(1024*1024*1024)
const idDelimiter = "/"

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
		"endpoint":    d.endpoint(),
		"userName":    d.userName(),
		"insecure":    d.insecure(),
		"volumePath":  d.volumePath(),
		"dataSubnets": d.dataSubnets(),
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

// Create an instance ID from a list of client IP addresses
func createInstanceId(clients []string) string {
	return strings.Join(clients, idDelimiter)
}

// Parse an instance ID into a list of client IP addresses
func parseInstanceId(id string) []string {
	return strings.Split(id, idDelimiter)
}

func (d *driver) GetInstance() (*core.Instance, error) {

	// parse the data subnet
	_, dataSubnet, err := net.ParseCIDR(d.dataSubnets())
	if err != nil {
		return nil, err
	}

	// find all local IP addresses on the data subnet
	ipList, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}
	var idList []string
	for _, addr := range ipList {
		ip, _, err := net.ParseCIDR(addr.String())
		if err != nil {
			return nil, err
		}
		if dataSubnet.Contains(ip) == true {
			idList = append(idList, ip.String())
		}
	}

	instance := &core.Instance{
		ProviderName: providerName,
		InstanceID:   createInstanceId(idList),
		Region:       "",
		Name:         "",
	}

	return instance, nil
}

func (d *driver) nfsMountPath(mountPath string) string {
	return fmt.Sprintf("%s:%s", d.nfsHost(), mountPath)
}

func (d *driver) GetVolumeMapping() ([]*core.BlockDevice, error) {

	exports, err := d.client.GetVolumeExports()
	if err != nil {
		return nil, err
	}

	var BlockDevices []*core.BlockDevice
	for _, export := range exports {
		
		device := &core.BlockDevice{
			ProviderName: providerName,
			InstanceID:   createInstanceId(export.Clients),
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
			attachmentSD := &core.VolumeAttachment{
				VolumeID:   volume.Name,
				InstanceID: blockDeviceMap[volume.Name].InstanceID,
				DeviceName: blockDeviceMap[volume.Name].DeviceName,
				Status:     "",
			}
			attachmentsSD = append(attachmentsSD, attachmentSD)
		}

		volSize, _ := d.getSize(volume.Name, volume.Name)

		volumeSD := &core.Volume{
			Name:             volume.Name,
			VolumeID:         volume.Name,
			Size:             strconv.FormatInt(volSize/bytesPerGb, 10),
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

	d.client.CreateVolume(volumeName)

	err := d.client.SetQuota(volumeName, size*bytesPerGb)
	if err != nil {
		// TODO: not sure how to handle this situation.  Delete created volume
		// and return an error?  Ignore and continue?
	}

	volumes, _ := d.GetVolume(volumeID, volumeName)

	return volumes[0], nil
}

func (d *driver) RemoveVolume(volumeID string) error {
	err := d.client.ClearQuota(volumeID)
	if err != nil {
		return err
	}

	err = d.client.DeleteVolume(volumeID)
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
	if volumeID == "" {
		return []*core.VolumeAttachment{}, errors.ErrMissingVolumeID
	}
	volume, err := d.GetVolume(volumeID, "")
	if err != nil {
		return []*core.VolumeAttachment{}, err
	}

	if instanceID != "" {
		for _, volumeAttachment := range volume[0].Attachments {
			if volumeAttachment.InstanceID == instanceID {
				return volume[0].Attachments, nil
			}
		}
		// not attached
		return []*core.VolumeAttachment{}, nil
	}
	return volume[0].Attachments, nil
}

func (d *driver) AttachVolume(
	notused bool,
	volumeID, instanceID string, force bool) ([]*core.VolumeAttachment, error) {

	// sanity check the input
	if volumeID == "" {
		return nil, errors.ErrMissingVolumeID
	}
	if instanceID == "" {
		return nil, goof.New("Missing Instance ID")
	}
	// ensure the volume exists and is exported
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
	// see if anyone is attached already
	clients, err := d.client.GetExportClients(volumeID)
	if err != nil {
		return nil, goof.WithError("problem getting export client", err)
	}
	
	// clear out any existing clients if necessary.  if force is false and 
	// we have existing clients, we need to exit.
	if len(clients) > 0 { 
		if force == false {
			return nil, goof.New("Volume already attached to another host")
		}
		
		// remove all clients
		err = d.client.ClearExportClients(volumeID)
		if err != nil {
			return nil, err
		}
	}

	err = d.client.SetExportClients(volumeID, parseInstanceId(instanceID))
	if err != nil {
		return nil, err
	}

	volumeAttachment, err := d.GetVolumeAttach(volumeID, instanceID)
	if err != nil {
		return nil, err
	}

	return volumeAttachment, nil

}

func (d *driver) DetachVolume(notUsed bool, volumeID string, blank string, notused bool) error {
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

	return nil
}

func (d *driver) CopySnapshot(
	runAsync bool,
	volumeID, snapshotID, snapshotName,
	destinationSnapshotName, destinationRegion string) (*core.Snapshot, error) {
	return nil, errors.ErrNotImplemented
}

func (d *driver) GetDeviceNextAvailable() (string, error) {
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

func (d *driver) dataSubnets() string {
	return d.r.Config.GetString("isilon.dataSubnets")
}

func configRegistration() *gofig.Registration {
	r := gofig.NewRegistration("Isilon")
	r.Key(gofig.String, "", "", "", "isilon.endpoint")
	r.Key(gofig.Bool, "", false, "", "isilon.insecure")
	r.Key(gofig.String, "", "", "", "isilon.userName")
	r.Key(gofig.String, "", "", "", "isilon.password")
	r.Key(gofig.String, "", "", "", "isilon.volumePath")
	r.Key(gofig.String, "", "", "", "isilon.nfsHost")
	r.Key(gofig.String, "", "", "", "isilon.dataSubnets")
	return r
}
