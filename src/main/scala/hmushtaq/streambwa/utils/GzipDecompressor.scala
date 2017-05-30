/*
 * Copyright (C) 2017 Hamid Mushtaq, TU Delft
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package hmushtaq.streambwa.utils

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.util.zip.GZIPInputStream
import org.apache.commons.io.IOUtils

class GzipDecompressor(compressed: Array[Byte])
{
	def decompressToBytes() : Array[Byte] =
	{
		val out = new ByteArrayOutputStream()
		IOUtils.copy(new GZIPInputStream(new ByteArrayInputStream(compressed)), out)
		return out.toByteArray
	}
	
	def decompress() : String = 
	{
		val out = new ByteArrayOutputStream()
		IOUtils.copy(new GZIPInputStream(new ByteArrayInputStream(compressed)), out)
		return out.toString("UTF-8")
	}
}
