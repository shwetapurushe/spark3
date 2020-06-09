Playing around with Spark stuff

* run the application with this command (first navigate to the target folder from within intellij Terminal)

```
spark-submit\
 --class myorg.XGBoost\
 --master local\
 /Users/spurushe/Documents/spark3/target/scala-2.12/spark3_2.12-0.1.jar
 ``` 
 
* Following any changes go to sbt_terminal and run the command 
```
package
``` 
This will compile the jar in the target folder again. 