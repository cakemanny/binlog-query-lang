package binlog.query

import java.io.{ InputStream, OutputStream, BufferedInputStream, ByteArrayOutputStream }
import java.net.InetSocketAddress
import java.nio.channels.{ ServerSocketChannel, SocketChannel, Channels }
import scala.annotation.tailrec

/**
 * A MySql 4.1 protocol server for the binlog-query language
 */
object Server {

  def main(args: Array[String]): Unit = {
    if (args.size > 0)
      serveForever(Some(args(0).toInt))
    else
      serveForever(None)
  }

  private def DEFAULT_PORT = 6032

  def serveForever(port: Option[Int]): Unit = {
    val ss = ServerSocketChannel.open()
    ss.bind(new InetSocketAddress("127.0.0.1", port.getOrElse(DEFAULT_PORT)))
    println(s"listening on port ${port.getOrElse(DEFAULT_PORT)}")

    val mainThread = Thread.currentThread
    def shutdown(): Unit = mainThread.interrupt

    try {
      while (true) {
        val s = ss.accept()
        new Thread(() => handleConnection(s, shutdown _)).start()
      }
    } catch {
      case e: java.lang.InterruptedException =>
        if (Thread.interrupted)
          println("Server shutdown")
      case e: java.nio.channels.ClosedByInterruptException =>
        if (Thread.interrupted) // clears the interrupted flag
          println("Server shutdown")
    } finally {
      ss.close()
    }
  }

  // Writes data in little-endian format as per mysql client/server protocol
  implicit class RichOutputStream(val stream: OutputStream) extends AnyVal {
    def writeByte(v: Int): Unit = stream.write(v)
    def writeShort(v: Int): Unit = {
      stream.write((v >>>  0) & 0xff)
      stream.write((v >>>  8) & 0xff)
    }
    def writeInt3(v: Int): Unit = {
      stream.write((v >>>  0) & 0xff)
      stream.write((v >>>  8) & 0xff)
      stream.write((v >>> 16) & 0xff)
    }
    def writeInt(v: Int): Unit = {
      stream.write((v >>>  0) & 0xff)
      stream.write((v >>>  8) & 0xff)
      stream.write((v >>> 16) & 0xff)
      stream.write((v >>> 24) & 0xff)
    }
    def writeLong(v: Long): Unit = {
      stream.write((v >>>  0).toInt & 0xff)
      stream.write((v >>>  8).toInt & 0xff)
      stream.write((v >>> 16).toInt & 0xff)
      stream.write((v >>> 24).toInt & 0xff)
      stream.write((v >>> 32).toInt & 0xff)
      stream.write((v >>> 40).toInt & 0xff)
      stream.write((v >>> 48).toInt & 0xff)
      stream.write((v >>> 56).toInt & 0xff)
    }

    def writeBytes(bytes: Array[Byte]): Unit = {
      stream.write(bytes, 0, bytes.length)
    }

    def writeNullTerm(s: String): Unit = {
      val utf8Bytes = s.getBytes("UTF-8")
      stream.write(utf8Bytes, 0, utf8Bytes.length)
      stream.write(0) // null terminator
    }

    def writeLength(length: Long): Unit = {
      if (length < 251) {
        writeByte(length.toInt)
      } else if (length < 65536) {
        writeByte(252)
        writeShort(length.toInt)
      } else if (length < 16777216) {
        writeByte(253)
        writeInt3(length.toInt)
      } else {
        writeByte(254)
        writeLong(length)
      }
    }

    def writeLengthString(s: String): Unit = {
      val utf8Bytes = s.getBytes("UTF-8")
      writeLength(utf8Bytes.length)
      stream.write(utf8Bytes, 0, utf8Bytes.length)
    }
  }

  def writePacket(out: OutputStream, buf: ByteArrayOutputStream, packetNumber: Int): Unit = {
    val length = buf.size
    if (length < (1 << 24)) {
      out.writeInt3(length)
      out.writeByte(packetNumber)
      buf.writeTo(out)
    } else {
      sys.error("TODO")
    }
  }
  def writePacket(out: OutputStream, buf: Array[Byte], packetNumber: Int): Unit = {
    val length = buf.size
    if (length < (1 << 24)) {
      out.writeInt3(length)
      out.writeByte(packetNumber)
      out.write(buf, 0, length)
    } else {
      sys.error("TODO")
    }
  }

  def readPacket(in: InputStream): (Int, Array[Byte]) = {
    val b1 = in.read()
    val b2 = in.read()
    val b3 = in.read()
    if (b1 < 0 || b2 < 0 || b3 < 0)
      throw new Exception("End of stream")
    val length = b1 | (b2 << 8) | (b3 << 16)
    assert(length < (1 << 24))
    val seqNum = in.read()
    println(s"reading packet $seqNum of length $length")
    val buf = Array.ofDim[Byte](length)
    in.read(buf)
    (seqNum,buf)
  }

  // keep track of the sequence number - because we have to
  class PacketChannel(val in: InputStream, val out: OutputStream) {
    var seqNum: Int = 0
    def write(buf: Array[Byte]): Unit = {
      writePacket(out, buf, seqNum)
      seqNum += 1
    }
    def write(buf: ByteArrayOutputStream): Unit = {
      writePacket(out, buf, seqNum)
      seqNum += 1
    }
    def read(): Array[Byte] = {
      val (theirSeqNum, result) = readPacket(in)
      if (theirSeqNum != seqNum)
        println(s"warning: our seqnum $seqNum, recvd seqNum: $theirSeqNum")
      seqNum += 1
      result
    }
  }

  def fieldDef(fieldName: String): ByteArrayOutputStream = {
    val buf = new ByteArrayOutputStream
    buf.writeLengthString("def")
    buf.writeLengthString("dbname")
    buf.writeLengthString("table_name")
    buf.writeLengthString("orig_table_name")
    buf.writeLengthString(fieldName) // fieldName
    buf.writeLengthString(fieldName) // orig fieldName
    // misc field data
    buf.writeByte(12) // length of field definition data
    buf.writeShort(CharacterCodes.utf8_general_ci)
    buf.writeInt(1 << 24) // max field length
    buf.writeByte(FieldType.MYSQL_TYPE_VAR_STRING)
    buf.writeShort(0) // field options
    buf.writeByte(0) // decimal point precision
    buf.writeShort(0) // reserved

    buf
  }

  // apply modifications to a new ByteArrayOutputStream and then
  // return the final buffer as a byte array
  def withBAOS(bufMod: ByteArrayOutputStream => Unit): Array[Byte] = {
    val buf = new ByteArrayOutputStream
    bufMod(buf)
    buf.toByteArray
  }

  def mkErrorPacket(code: Int, msg: String): ByteArrayOutputStream = {
    val buf = new ByteArrayOutputStream
    buf.writeByte(0xff) // packet_type=err
    buf.writeShort(code) // error code
    buf.writeBytes("#HY000".getBytes("UTF-8"))
    buf.writeNullTerm(msg)
    buf
  }

  private def handleConnection(s: SocketChannel, shutdown: () => Unit): Unit = {
    try {
      val out = Channels.newOutputStream(s)
      val in = new BufferedInputStream(Channels.newInputStream(s))

      {
        val buf = new java.io.ByteArrayOutputStream
        // write greeting
        buf.writeByte(0x0a) // protocol_version_number
        buf.writeNullTerm("5.6.30") // version_string
        buf.writeInt(Thread.currentThread.getId.toInt) // thread ID
        buf.writeNullTerm("12345678") // random seed pt1

        val capabilities = (
            Capabilities.CLIENT_LONG_FLAG
          | Capabilities.CLIENT_CONNECT_WITH_DB
          | Capabilities.CLIENT_PROTOCOL_41
          | Capabilities.CLIENT_SECURE_CONNECTION
        )
        buf.writeShort(capabilities)
        buf.writeByte(CharacterCodes.utf8_general_ci) // default collation
        buf.writeShort(StatusFlags.SERVER_STATUS_AUTOCOMMIT) // server status
        buf.writeBytes(Array.ofDim[Byte](13)) // reserved
        buf.writeNullTerm("901234567890") // rand_seed_pt2

        writePacket(out, buf, 0)
      }

      // read packet with username and password
      // (don't really care)
      val (_,response) = readPacket(in)

      val okPacket = Array[Byte](
        0, // packet type=OK
        0, // num rows changed - variable length int
        0, // last_insert_id - variable length int
        2, 0, // server status
        0, 0 // status message | warnings
      )
      val eofPacket = Array[Byte](
        0xfe.asInstanceOf[Byte], // packet_type=EOF
        0, 0, // number of warnings
        2, 0 // server status
      )
      writePacket(out, okPacket, 2)

      @tailrec def commandLoop(): Unit = {
        import CommandType._
        val channel = new PacketChannel(in, out) // starts channel at seqnum 0

        val command = channel.read()
        val commandType = command(0)
        commandType match {
          case COM_QUERY =>
            val query = new String(command, 1, command.size - 1)
            println("Query: " + query)

            // Convenience for writing trivial resultsets
            def writeResultSet(
              fields: Vector[String], rows: Vector[Vector[String]]
            ): Unit = {
              // Number of fields - length encoded
              channel.write(withBAOS(_.writeLength(fields.size)))
              fields.foreach { fieldName => channel.write(fieldDef(fieldName)) }
              channel.write(eofPacket)
              // Send the data
              rows.foreach { row =>
                channel.write(withBAOS { buf =>
                  row.foreach { value =>
                    buf.writeLengthString(value)
                  }
                })
              }
              channel.write(eofPacket)
            }

            query match {
              case "error" =>
                channel.write(mkErrorPacket(1000, "An error has occurred"))
                commandLoop()
              case "test" =>
                writeResultSet(Vector("fieldname", "fieldname2"),
                  Vector(
                    Vector("hello", "there"),
                    Vector("how are", "you")
                  )
                )
                commandLoop()
              // work around to allow mysql workbench to connect
              // ignore all it's set transaction isolation level shit
              case s if s.startsWith("set ") || s.startsWith("SET ") =>
                channel.write(okPacket)
                commandLoop()
              case "SELECT current_user()" =>
                writeResultSet(
                  Vector("current_user()"),
                  Vector(Vector("root@localhost"))
                )
                commandLoop()
              case "SELECT CONNECTION_ID()" =>
                writeResultSet(
                  Vector("CONNECTION_ID()"),
                  Vector(Vector(Thread.currentThread.getId.toString))
                )
                commandLoop()
              case "SHOW SESSION STATUS LIKE 'Ssl_cipher'" =>
                writeResultSet(Vector("Variable_name", "Value"), Vector())
                commandLoop()
              case "SHOW DATABASES" =>
                writeResultSet(Vector("Database"), Vector())
                commandLoop()
              case s if s startsWith "SHOW SESSION VARIABLES" =>
                // empty result set
                writeResultSet(Vector("Variable_name", "Value"), Vector())
                commandLoop()
              // And the guys that make heidi work
              case "SHOW STATUS" =>
                writeResultSet(Vector("Variable_name", "Value"), Vector())
                commandLoop()
              case "SHOW VARIABLES" =>
                writeResultSet(Vector("Variable_name", "Value"), Vector())
                commandLoop()
              case _ => // Our actual query execution code
                try {
                  Main.runQuery(query){ header =>
                    channel.write({
                      val buf = new ByteArrayOutputStream
                      buf.writeLength(header.size)
                      buf
                    })
                    header.foreach { fieldName =>
                      channel.write(fieldDef(fieldName))
                    }
                    channel.write(eofPacket)
                  }{ row =>
                    import QueryAST._
                    val buf = new ByteArrayOutputStream
                    row.foreach{
                      case StrL(s) => buf.writeLengthString(s)
                      case LongL(l) => buf.writeLengthString(l.toString)
                      case DoubleL(d) => buf.writeLengthString(d.toString)
                      case NullL => buf.writeByte(251)
                    }
                    channel.write(buf)
                  }
                  channel.write(eofPacket)
                } catch {
                  case e: Exception =>
                    // maybe do stacktrace?
                    channel.write(mkErrorPacket(1000, "Error: " + e.getMessage))
                }
                commandLoop()
            }
          case COM_QUIT =>
            channel.write(okPacket)
          case COM_SHUTDOWN =>
            channel.write(okPacket)
            shutdown()
          case _ =>
            println(s"Received command: $commandType")
            channel.write(okPacket)
            commandLoop()
        }
      }
      commandLoop()

    } finally {
      s.close()
    }
  }

}

