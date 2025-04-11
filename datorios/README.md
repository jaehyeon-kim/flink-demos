[![](https://github.com/metrolinkai/Datorios/blob/main/resources/Horizontal%20Positive.png)](https://datorios.con "See The Data Behind Your Data - Data Observability for Apache Flink")



# See The Data Behind Your Data - Data Observability for Apache Flink ® 

### **Unified Investigation Platform:**
When all of your pipeline data is available in one place, you no longer have the need to waste time flipping between different places to access source data, application logging, and pipeline metrics. The precious time spent flipping between platforms could be time spent bringing your pipelines back to a healthy state.

### **Effortless Debugging of Operator Functionality & State:**
See every record that passed through each of your operators, unlocking the ability to see how the record altered state while it was being processed.

### **Better Integration Testing:**
A lot is left to be desired when testing your pipelines. Unit testing only gives us half the story, by seeing how all of your operators work together when the data flows you can be confident in the results provided by your pipelines.

### **Peace of Mind in Production Monitoring:**
Access to your mission-critical performance metrics gives you the peace of mind knowing your jobs are resourced correctly and won't fall over when more data starts flowing in.

### **Breeze Through Window Investigation:**
With the Window Investigation tool you can magnify the problems you are encountering with windowing. Problems can include late events, incorrect watermark settings, or even your aggregation functions.

# **High level Architecture:** 
Datorios consists of two components:


- Datorios client running on Docker Compose - The client will install the Apache Flink engine on your local/cloud machine, where your jobs will be deployed (embedding Datorios to your current Flink is coming up soon).
- A cloud observability service is used for deep investigation and debugging.

![](https://github.com/metrolinkai/Datorios/blob/main/resources/image-20240425-111715.png)

[Signup](https://app.datorios.com/signup) to download the Datorios cluster - You can use your own or the demo Flink jobs in [this repository](https://github.com/metrolinkai/Datorios/tree/main/flink-examples) for a test run

# **K8S Architecture:** 

![](https://github.com/metrolinkai/Datorios/blob/main/resources/K8K%20Architecture%203.png)

[SaaS side](https://github.com/metrolinkai/Datorios/blob/main/resources/SAAS.drawio.png)

[![](https://github.com/metrolinkai/Datorios/blob/main/resources/Copy%20of%20squirrel%20xray%20(1).png)](https://datorios.con "Making your Flink transparent")
