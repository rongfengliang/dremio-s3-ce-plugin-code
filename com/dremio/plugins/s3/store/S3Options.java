package com.dremio.plugins.s3.store;

import com.dremio.options.Options;
import com.dremio.options.TypeValidators.BooleanValidator;

@Options
public class S3Options {
   public static final BooleanValidator ASYNC = new BooleanValidator("store.s3.async", true);
}
