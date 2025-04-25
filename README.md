### Expected Result
Landing
    Customer: 956
    Accelerometer: 81273
    Step Trainer: 28680
Trusted
    Customer: 482
    Accelerometer: 40981
    Step Trainer: 14460
Curated
    Customer: 482
    Machine Learning: 4368


Looking at the expected result for machine_learning_curated, its appeared to be incorrect.
By only joining accelerometer data with trainer data on timestamp, this will yield the 'coorect' result
However I firmly believe that we would get a more acurate data by also ensuring that the serial number matches up between accelerometer and step trainer data.


