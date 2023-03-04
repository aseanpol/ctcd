# Changelog
// timeStamp, lat, lng, speed, acc, sdStatus, fuel, angle, distance, driver_name, driver_license, isOverSpeed, drivingElapse, isSessionOvertime, dayStretch

03/03/23:
- MqttMessage : dùng mảng byte rồi check để lấy string
- image payload : 0xFF 0xD8 . . . . 0xFF 0xD9;chuỗi unixTime,length : lấy khoảng 30 byte cuôi cùng, tìm ký tự ';', sau ký tự ; là nội dung header
- sửa file config.xml : trong thẻ watchdog thêm images_path
16/02/23:
- khi calib lỗi thì try catch -> vẫn xử lý location và fuel=-1

19/01/23:
- fw v1.6.1 đã add các feature về network
{"id":"ttc0_f412fa4313b8","time":1674101346,"type":"network","phone_number":"84369370621","operator":"Viettel","rssi":19,"imsi":"452048825588958","iccid":"89840480088255889583"}


04/01/23:
- elapse bị sai trong case : miss gps

31/12/22:
- vtrackd có sửa processPower

30/12/22:
- db : split_log_record_by_device : clear day_elapse trong device và moving_elapse trong device_log
- bỏ khái niệm movingElapse trong processPower, nếu update session thì cộng dồn vào cái cũ
- bỏ moving_elapse trong device_log
- trong 4 case : chỉ làm case moving->moving thì movingElaspe chỉ có ý nghĩa để cộng dồn vào day_elapse của device (vẫn phải cần ACC)
- khi lấy lastInsertRecord st.getGeneratedKeys() : có mã mới update, không có mã không được update, chỉ sửa vtrackd processPower, chạy được thì sửa trong ctcd -> đã sửa luôn ctcd
-> sửa luôn lastInsertID trong các method, nếu success thì quay lại sửa trong vtrackd (vtrackd phải giữ lastInsertId lại như cũ)
- vtrackd cho thêm finally đóng cs, st, con
- 
29/12/22:
- bảng power : khi update record phân ra 2 trường hợp : nếu ON thì update movingElapse, nếu OFF thì không
- day_elapse trong bảng device nó chỉ lưu gía trị movingElapse gần nhất -> đã thêm biến dayMovingElapse -> sau khi split không clear day_elapse

28/12/22:
- moving_elapse : tính thêm case moving->stop
- nếu qua ngày thì tạo record mới trong power
- có bug : khi tạo mới record power qua ngày thì nó vẫn giữ nguyên giá trị elapse

22/12/22:
- khôi phục lại day_elaspe trong db : chỉ tính khi moving to moving, 3 case khác chỉ lưu
- db đã tăng thêm 1 cột moving_elapse trong bảng power, đã add cho rts và rtslog
- summary_report tái sử dụng cột power_off_count làm moving_elapse
- sửa lại algorithm : trong device_log chỉ lưu đúng moving_elapse chứ không lưu cả ngày, chỉ có device mới lưu moving cả ngày
case bắt đầu moving : cho moving_elapse=0
case đang moving : cộng dồn như hiện tại
case moving -> stop : update moving_elapse vào device : không làm
- chưa xử lý được bảng power cột moving_elapse : làm sao để cộng dồn các record khi moving->R
- còn : khi bắt đầu move không tính moving_elapse -> xem lại
- khi có event : update version : đang comment

04/11/22:
- đã đổi cấu trúc project, bỏ module

18/10/22:
- đã fix summary_report
- bảng device_log : đổi tên cột driver_elapse -> day_stretch
- processLocation : 3 case (trừ StopToStop) đều phải update hoặc insert stretch vào cột day_stretch của device_log, làm song song với device

16/10/22:
- add fixGPS, dùng lại dayStretch
- khi validateIMEI : bỏ day_elapse

29/09/22:
- phiên bản ttc-cam chay thử với thư viện rabbitmq, không dùng paho : vì pahoo sau 1 thời gian thì không nhận data
- thay dần các listener và method của paho

20/09/22:
- fuel calib : tạo bảng calib trong DB
- device có thêm cột tank_height(int), calib(boolean) : check trước calib, nếu true thì bỏ qua tank height, nếu không cần calib thì căn cứ tank_height
- sửa các funtion liên quan tính summary : calculate_oil_report, . . .
- bảng device_log : cũng sửa tiền tố oil_
- khi query thông device device : lấy clientTaxCode của tài khoản quản lý (taxCode của đơn vị vận tải)
lưu value taxCode vào Tracker
- trong bảng slave_account, master_account db đổi tên cột mst thành tax_code
- file cấu hình : đổi tên thuộc tính mst -> taxcode

31/08/22:
- tạo thêm cột amqp_success trong bảng mileage, có hoán đổi vị trí cột, check lại ctcd
- vì gởi amqp theo bảng mileage nên tạo thêm mileageID trong thuộc tính tracker
- kiểm tra lại vtrackd
- đã test gởi amqp đến DRVN tốt

30/08/22:
- tích hợp 2 lib : protobuf-java.jar và rabbitmq-client.jar
- gởi AMQP đến server

26/08/22:
- có bug : device gởi không có distance, arr[8] là distance, server phải tự tính

23/08/22:
- chuyển isLegalTime() vào trong Tracker
- khi offline thi isLegalTime=false -> elapse=0

16/08/22 :
- tạo overspeed violation
- tạo bảng overspeed_violation : os_violation_id
- trong class Tracker có 2 biến khác nhau : speedLimit, overSpeed và overSpeedViolation. overSpeed do device tính toán và gởi đến server, speedLimit do server lưu sẵn, overSpeedViolation là server tính toán vi phạm theo TT09
- sửa lại ý nghĩa elapse : là khoảng time lần cuối cùng gởi msg và time của lần hiện tại, tức là end_time trong DB và getTime của tracker. Với trường hợp OFFLINE MSG, elapse sẽ rối tung, nên khi đó = 0
- khi giả lập speed thì device không tính được overspeed, nên server sẽ nhận được false
- cột driving_id trong bảng device sẽ bỏ
- có 2 trường hợp sẽ xóa biến speed_vio_id trong bảng device : khi stop hoặc khi không bị violation, record trong bảng speed_violation đã tự tính realtime rồi, không cần cho end
-> chỉ có 1 case MovingToMoving là xử lý, còn 3 case kia kiểm tra speed_vio_id trong device, nếu > 0 thì clear=0
- bổ sung thêm duration để khác biệt với elapse : khi stop thì duration tính từ beginTime đến hiện tại của tracker

30/07/22:
- tạo bảng mileage : dùng cho report hành trình xe chạy, tách data từ location ra, lấy đúng lat, lng, receive_time cần thiết
- tạo thêm method updateMileage : insert vào bảng mileage

25/07/22:
- MIN_MOVING_SPEED=5 giống như trong device

22/07/22:
- L03.TXT : parking có sửa lại start_time là thời điếm bắt đầu parking, nên lấy value này để show GUI là thời điểm stop/park
không được lấy time lúc gởi msg làm thời điểm stop/park
- method updateQCVNPark : trong DB không nên thêm cột, chỉ tận dụng cột receive_time làm cột start_time khi parking

18/07/22:
- sự kiện driving_operation : khi tag thẻ RFID, lưu vào bảng tracker_event, lấy result là session=login/logout

04/06/22:
- device_log : nếu đang moving : không update end_time của record trước đó, nếu đang moving->stop cũng không update end_time
- stopToMoving : chỉ update end_time cho prev record khi speed<MOVING_SPEED
- đã fix bug các record history, tracking bị trùng thời gian và trạng thái

03/06/22:
- bảng driving_op không có cột receive_time, chèn vào trước description
- TrackingAdapter QCVN driving : add thêm data vào cột receive_time
- bảng driving_op có thêm các thông tin : stretch để server query theo vehicle, driver
- bảng overspeed thêm cột stretch trước description : quãng đường đã overspeed trong hơn 20s

02/06/22:
- server không tính bearing + distance, lấy từ device, comment line 366, 369, 370 của TrackerAdapter
- xử lý item angle và distance
// timeStamp, lat, lng, speed, acc, sdStatus, fuel, angle, distance, driver_name, driver_license, isOverSpeed, drivingElapse, isSessionOvertime, dayStretch
- bỏ bước prepareLocation
- chưa bỏ driving trong location được
- thay thế door = sd nhưng chưa sửa trong DB được
- tạm thời comment tất cả process liên quan oil, aircon, truck
- bỏ xử lý bảng driving
- bỏ day_stretch và day_elapse trong processMoving
- location đã sửa các item theo firmware 02/06/22
- khi insert hay update DB : bỏ unix_timestamp(now()), chuyển sang lấy unixTime của device
- xem kỹ -begin_time
- bỏ phần tracking : "Dừng . . ."
- khi location : chỉ chia 2 trường hợp đang moving hoặc đang stop
- moving thì insert table, không update record cũ
- setDriverElapse : không dùng cột driver_elapse trong bảng device_log, sẽ dùng driving_elapse
- getDrivingId : đã comment hết
- day_stretch : km trong ngày, sẽ lấy data từ device, server không tính, lưu vào bảng device
- 

30/05/22:
- bảng device_log đã sửa cấu trúc : thêm 2 cột gần cuối nhưng chỉ sửa ctcd, các service khác như vtrackd không sửa
- nên phải đặt default value = 0 để không ảnh hưởng tới các service khác
- chuyển isOverspeed ra cuối, trước drivingElapse 

29/05/22:
- tạo bảng driving_op
- 1653819770,16.063391,108.169991,0,0,0,0,4095,86101110195,86101110195
- timeStamp, lat, lng, speed, overSpeed, acc, door, fuel, driver_name, driver_license, drivingElapse, isOvertime
- bảng device_log : add thêm 2 cột vào cuối cùng : driving_elapse, is_overtime

25/05/22:
- location : add thêm overSpeed sau speed và trước acc
timeStamp, lat, lng, speed, overSpeed, acc, door, fuel, driver_name, driver_license
- location : case mất GPS tạm thời cho phép update over_speed line 1241
- xử lý msg overspeed trong topic qcvn -> insert bảng overspeed

23/05/22:
- đã xử lý bảng speed

10/05/22:
- sửa TrackerAdapter : updateTime() khi miss GPS vẫn lưu các sự kiện IO và driver

03/05/22:
- xử lý msg event lưu db
- sửa cấu trúc bảng tracker_event trong mysql

31/03/2022:
- tách ra 5 trường hợp tương ứng 5 msg : location, event, firmware, command, qcvn
- phân biệt 2 trường hợp version cũ và version mới (>=8 item)
- xử lý các item theo SRS firmware 11032022

Tạo ngày 06/01/2022