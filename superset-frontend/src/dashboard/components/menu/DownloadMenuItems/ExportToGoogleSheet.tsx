/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import React, { useEffect } from 'react';
import { FeatureFlag, isFeatureEnabled, logging, t } from '@superset-ui/core';
import { Menu } from 'src/components/Menu';
import Icons from '../../../../components/Icons';
import { getExportGoogleSheetsUrl } from '../../../util/exportToGoogleSheet';

export default function ExportToGoogleSheet({
  logEvent,
  dashboardId,
  addDangerToast,
  ...rest
}: {
  addDangerToast: Function;
  dashboardId: number;
  logEvent?: Function;
}) {
  if (!isFeatureEnabled(FeatureFlag.GoogleSheetsExport)) {
    return <></>;
  }

  const handleGoogleSheetsExport = async () => {
    try {
      const googleSheetUrl = await getExportGoogleSheetsUrl(dashboardId);
      window.open(googleSheetUrl, '_blank')?.focus();
    } catch (error) {
      logging.error(error);
      addDangerToast(t('Sorry, something went wrong. Try again later.'));
    }
  };

  return (
    <Menu.Item key="google-sheets" onClick={handleGoogleSheetsExport} {...rest}>
      <Icons.GoogleOutlined /> {t('Google Sheets')}
    </Menu.Item>
  );
}
