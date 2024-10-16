package ai.onehouse.lakeview.sync.utilities;

import com.beust.jcommander.converters.IParameterSplitter;

import java.util.Collections;
import java.util.List;

public class IdentitySplitter implements IParameterSplitter {
  public List<String> split(String value) {
    return Collections.singletonList(value);
  }
}
