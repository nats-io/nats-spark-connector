package natsconnector

import io.nats.client.AuthHandler
import io.nats.client.NKey

import java.io.BufferedReader
import java.io.File
import java.io.FileReader
import java.io.IOException
import java.security.GeneralSecurityException

import java.util.Arrays
import java.security.GeneralSecurityException

class SampleAuthHandler(path:String) extends AuthHandler {
    private val nkey:NKey = aquireNKey() 

    private def aquireNKey():NKey = {
        val f = new File(path)
        val numBytes:Int = (f.length()).toInt
        var nKey:NKey = null
        try  {
            val in = new BufferedReader(new FileReader(f))
            var buffer:Array[Char] = new Array(numBytes)
            val numChars:Int = in.read(buffer)
            if (numChars < numBytes) {
                var seed:Array[Char] = new Array(numChars)
                System.arraycopy(buffer, 0, seed, 0, numChars)
                nKey = NKey.fromSeed(seed);
                Arrays.fill(seed, '\0') // clear memory
            }
            else {
                nKey = NKey.fromSeed(buffer);
            }
            Arrays.fill(buffer, '\0') // clear memory
        }
        catch {
            case ex: Exception => { println(s"Got security exception when acquiring NKey: ${ex.getMessage()}"); System.exit(-1)}
        }
        nKey

    }

    def getNKey():NKey = {
        this.nkey
    }

    def getID():Array[Char] =  {
        try {
            return this.nkey.getPublicKey()
        } 
        catch {
            case ex: GeneralSecurityException => return null
            case ex: IOException => return null
            case ex: NullPointerException => return null
        }
    }

    def sign(nonce:Array[Byte]):Array[Byte] = {
        try {
            return this.nkey.sign(nonce)
        }
        catch {
            case ex: GeneralSecurityException => return null
            case ex: IOException => return null
            case ex: NullPointerException => return null
        }
   }

    def getJWT():Array[Char] = {
        return null;
    }
}
