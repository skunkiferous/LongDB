@ECHO OFF

IF "%PB_ROOT%"=="" set PB_ROOT=C:\work\workspace\LongDB
IF "%JAVA_HOME%"=="" set JAVA_HOME=c:\Progra~2\Java\jdk1.7.0_03
IF "%MAVEN_REPO%"=="" set MAVEN_REPO=%USERPROFILE%\.m2\repository

set CP=
set CP=%MAVEN_REPO%\com\blockwithme\API\0.0.1-SNAPSHOT\*
set CP=%CP%;%MAVEN_REPO%\com\blockwithme\APIClient\0.0.1-SNAPSHOT\*
set CP=%CP%;%MAVEN_REPO%\com\blockwithme\BaseImpl\0.0.1-SNAPSHOT\*
set CP=%CP%;%MAVEN_REPO%\com\blockwithme\BerkeleyDBImpl\0.0.1-SNAPSHOT\*
set CP=%CP%;%MAVEN_REPO%\com\blockwithme\CassandraEmbedded\0.0.1-SNAPSHOT\*
set CP=%CP%;%MAVEN_REPO%\com\blockwithme\CassandraImpl\0.0.1-SNAPSHOT\*
set CP=%CP%;%MAVEN_REPO%\com\blockwithme\CassandraServerSide\0.0.1-SNAPSHOT\*
set CP=%CP%;%MAVEN_REPO%\com\blockwithme\DBTools\0.0.1-SNAPSHOT\*
set CP=%CP%;%MAVEN_REPO%\com\blockwithme\H2Impl\0.0.1-SNAPSHOT\*
set CP=%CP%;%MAVEN_REPO%\com\blockwithme\LevelDBJavaImpl\0.0.1-SNAPSHOT\*
set CP=%CP%;%MAVEN_REPO%\com\blockwithme\MemoryImp\0.0.1-SNAPSHOT\*
set CP=%CP%;%MAVEN_REPO%\com\blockwithme\VoltDBImpl\0.0.1-SNAPSHOT\*

set CP=%CP%;%MAVEN_REPO%\org\antlr\antlr\3.4\*
set CP=%CP%;%MAVEN_REPO%\org\antlr\antlr-runtime\3.4\*
set CP=%CP%;%MAVEN_REPO%\org\antlr\stringtemplate\3.2.1\*
set CP=%CP%;%MAVEN_REPO%\antlr\antlr\2.7.7\*
set CP=%CP%;%MAVEN_REPO%\org\antlr\ST4\4.0.4\*
set CP=%CP%;%MAVEN_REPO%\commons-configuration\commons-configuration\1.8\*
set CP=%CP%;%MAVEN_REPO%\commons-lang\commons-lang\2.6\*
set CP=%CP%;%MAVEN_REPO%\commons-io\commons-io\2.3\*
set CP=%CP%;%MAVEN_REPO%\commons-logging\commons-logging\1.1.1\*
set CP=%CP%;%MAVEN_REPO%\com\googlecode\concurrentlinkedhashmap\concurrentlinkedhashmap-lru\1.2\*
set CP=%CP%;%MAVEN_REPO%\com\github\stephenc\high-scale-lib\high-scale-lib\1.1.4\*
set CP=%CP%;%MAVEN_REPO%\com\carrotsearch\hppc\0.4.1\*
set CP=%CP%;%MAVEN_REPO%\org\slf4j\slf4j-api\1.6.4\*
set CP=%CP%;%MAVEN_REPO%\commons-cli\commons-cli\1.2\*
set CP=%CP%;%MAVEN_REPO%\com\google\guava\guava\12.0\*
set CP=%CP%;%MAVEN_REPO%\com\google\code\findbugs\jsr305\1.3.9\*
set CP=%CP%;%MAVEN_REPO%\commons-codec\commons-codec\1.6\*
set CP=%CP%;%MAVEN_REPO%\org\codehaus\jackson\jackson-core-asl\1.4.0\*
set CP=%CP%;%MAVEN_REPO%\org\apache\cassandra\cassandra-all\1.0.8\*
set CP=%CP%;%MAVEN_REPO%\org\xerial\snappy\snappy-java\1.0.3\*
set CP=%CP%;%MAVEN_REPO%\com\ning\compress-lzf\0.8.4\*
set CP=%CP%;%MAVEN_REPO%\org\apache\cassandra\deps\avro\1.4.0-cassandra-1\*
set CP=%CP%;%MAVEN_REPO%\org\mortbay\jetty\jetty\6.1.22\*
set CP=%CP%;%MAVEN_REPO%\org\mortbay\jetty\jetty-util\6.1.22\*
set CP=%CP%;%MAVEN_REPO%\org\mortbay\jetty\servlet-api\2.5-20081211\*
set CP=%CP%;%MAVEN_REPO%\org\codehaus\jackson\jackson-mapper-asl\1.4.0\*
set CP=%CP%;%MAVEN_REPO%\jline\jline\0.9.94\*
set CP=%CP%;%MAVEN_REPO%\com\googlecode\json-simple\json-simple\1.1\*
set CP=%CP%;%MAVEN_REPO%\org\yaml\snakeyaml\1.6\*
set CP=%CP%;%MAVEN_REPO%\log4j\log4j\1.2.16\*
set CP=%CP%;%MAVEN_REPO%\org\slf4j\slf4j-log4j12\1.6.1\*
set CP=%CP%;%MAVEN_REPO%\org\apache\thrift\libthrift\0.6.1\*
set CP=%CP%;%MAVEN_REPO%\junit\junit\4.10\*
set CP=%CP%;%MAVEN_REPO%\org\hamcrest\hamcrest-core\1.1\*
set CP=%CP%;%MAVEN_REPO%\javax\servlet\servlet-api\2.5\*
set CP=%CP%;%MAVEN_REPO%\org\apache\httpcomponents\httpclient\4.0.1\*
set CP=%CP%;%MAVEN_REPO%\org\apache\httpcomponents\httpcore\4.0.1\*
set CP=%CP%;%MAVEN_REPO%\org\apache\cassandra\cassandra-thrift\1.0.8\*
set CP=%CP%;%MAVEN_REPO%\com\github\stephenc\jamm\0.2.5\*
set CP=%CP%;%MAVEN_REPO%\me\prettyprint\hector-core\1.0-4\*
set CP=%CP%;%MAVEN_REPO%\commons-pool\commons-pool\1.5.3\*
set CP=%CP%;%MAVEN_REPO%\com\github\stephenc\eaio-uuid\uuid\3.2.0\*
set CP=%CP%;%MAVEN_REPO%\com\ecyrd\speed4j\speed4j\0.9\*
set CP=%CP%;%MAVEN_REPO%\com\h2database\h2\1.3.166\*
set CP=%CP%;%MAVEN_REPO%\org\voltdb\voltdbclient\2.7.2\*
set CP=%CP%;%MAVEN_REPO%\com\sleepycat\je\4.0.92\*
set CP=%CP%;%MAVEN_REPO%\org\iq80\leveldb\leveldb\0.3\*
set CP=%CP%;%MAVEN_REPO%\org\iq80\leveldb\leveldb-api\0.3\*

set CP=%CP%;%MAVEN_REPO%\com\google\inject\guice\3.0\*
set CP=%CP%;%MAVEN_REPO%\com\google\inject\extensions\guice-multibindings\3.0\*
set CP=%CP%;%MAVEN_REPO%\javax\inject\javax.inject\1\*
set CP=%CP%;%MAVEN_REPO%\aopalliance\aopalliance\1.0\*

set CLASSPATH=%CP%;

"%JAVA_HOME%\bin\java.exe" -Xmx1024m -Dlog4j.configuration=file:///%PB_ROOT%\DBTools\resources\log4j-dbtool.properties com.blockwithme.longdb.tools.DBTool %*