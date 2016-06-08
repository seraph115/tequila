name := "tequila"

version := "0.0.1"

scalaVersion := "2.11.7"

resolvers += "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"

resolvers += "Bigbata" at "http://nexus.bigbata.com/nexus/content/groups/public/"

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "1.6.1"

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "1.6.1"

libraryDependencies += "org.apache.spark" % "spark-hive_2.11" % "1.6.1"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "1.6.1"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.2"

libraryDependencies += "org.ansj" % "ansj_seg" % "3.0"

libraryDependencies += "org.nlpcn" % "nlp-lang" % "1.0.2"

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.36"
